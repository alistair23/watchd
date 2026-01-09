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

pub mod air_quality_openaq;
pub mod calendar;
pub mod cobs;
pub mod communicator;
pub mod data_transfer;
pub mod garmin_json;
pub mod garmin_weather_api;
pub mod http;
pub mod messages;
pub mod mlr;
pub mod protobuf_calendar;
pub mod types;
pub mod watchdog;
pub mod weather;
pub mod weather_bom;
pub mod weather_provider;

use bluer::{gatt::remote::Characteristic, Adapter, Address, Device, Session};
use calendar::CalendarManager;
use clap::Parser;
use communicator::{
    AsyncGfdiMessageCallback, BleSupport, CharacteristicHandle, CommunicatorV2, Transaction,
};
use data_transfer::DataTransferHandler;
use futures::stream::StreamExt;
use http::{handle_http_request_with_weather, HttpRequest};
use log::{debug, error, info};
use messages::{GfdiMessage, MessageGenerator, MessageParser};
use protobuf_calendar::{
    encode_calendar_response, handle_calendar_request, parse_calendar_request,
    CalendarResponseStatus,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use types::GarminError;
use watchdog::{HealthStatus, WatchdogConfig, WatchdogManager};
use weather_provider::{UnifiedWeatherProvider, WeatherProviderType};
use zbus::{message::Message, Connection};

/// Helper function to format bytes as hex for debugging
fn hex_dump(data: &[u8], max_len: usize) -> String {
    let len = data.len().min(max_len);
    let hex: String = data[..len]
        .iter()
        .map(|b| format!("{:02X}", b))
        .collect::<Vec<_>>()
        .join(" ");
    if data.len() > max_len {
        format!("{} ... ({} bytes total)", hex, data.len())
    } else {
        format!("{} ({} bytes)", hex, data.len())
    }
}

/// Compute CRC-16 checksum for Garmin messages (CRC-16/MODBUS)
fn compute_crc16(data: &[u8]) -> u16 {
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

/// Command line arguments
#[derive(Parser, Debug)]
#[command(name = "notification_dbus_monitor")]
#[command(about = "Monitor DBus notifications and forward to Garmin watch")]
struct Args {
    /// Bluetooth MAC address of the Garmin device (format: AA:BB:CC:DD:EE:FF)
    #[arg(value_name = "MAC_ADDRESS")]
    mac_address: String,

    /// Filter: only forward notifications from specific apps (comma-separated)
    #[arg(long)]
    filter_apps: Option<String>,

    /// Minimum urgency level (0=low, 1=normal, 2=critical)
    #[arg(long, default_value = "0")]
    min_urgency: u8,

    /// Enable duplicate notification detection (filters repeated content)
    #[arg(long)]
    enable_dedup: bool,

    /// Enable calendar sync from GNOME Calendar / KOrganizer / URLs
    #[arg(long)]
    enable_calendar_sync: bool,

    /// Calendar sync interval in minutes (default: 15)
    #[arg(long, default_value = "15")]
    calendar_sync_interval: u64,

    /// Number of days to look ahead for calendar events (default: 7)
    #[arg(long, default_value = "7")]
    calendar_lookahead_days: i64,

    /// Calendar URLs to fetch ICS files from (comma-separated)
    /// Example: https://calendar.google.com/calendar/ical/.../basic.ics
    #[arg(long)]
    calendar_urls: Option<String>,

    /// Cache duration for downloaded calendar ICS files in seconds (default: 300)
    #[arg(long, default_value = "300")]
    calendar_cache_duration: u64,
}

// ============================================================================
// DBus Notification Types
// ============================================================================

#[allow(dead_code)]
#[derive(Debug)]
struct DbusNotification {
    app_name: String,
    replaces_id: u32,
    app_icon: String,
    summary: String,
    body: String,
    actions: Vec<String>,
    hints: HashMap<String, zbus::zvariant::OwnedValue>,
    timeout: i32,
}

// ============================================================================
// Call Specification Types (same as phone_call_notification.rs)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CallCommand {
    Undefined = 0,
    Accept = 1,
    Incoming = 2,
    Outgoing = 3,
    Reject = 4,
    Start = 5,
    End = 6,
}

#[derive(Debug, Clone)]
pub struct CallSpec {
    pub number: String,
    pub name: Option<String>,
    pub source_name: Option<String>,
    pub source_app_id: Option<String>,
    pub command: CallCommand,
}

impl CallSpec {
    pub fn new(number: String, command: CallCommand) -> Self {
        Self {
            number,
            name: None,
            source_name: None,
            source_app_id: None,
            command,
        }
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn get_id(&self) -> i32 {
        self.number
            .bytes()
            .fold(0i32, |acc, b| acc.wrapping_mul(31).wrapping_add(b as i32))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum NotificationType {
    GenericPhone = 1,
    GenericSms = 2,
    GenericEmail = 3,
    GenericChat = 4,
    GenericSocial = 5,
    GenericNavigation = 6,
    GenericCalendar = 7,
    GenericAlarmClock = 8,
    Generic = 9,
}

impl NotificationType {
    /// Get the category value for Garmin devices
    fn category_value(&self) -> u8 {
        match self {
            NotificationType::GenericPhone => 1,       // INCOMING_CALL
            NotificationType::GenericEmail => 6,       // EMAIL
            NotificationType::GenericSms => 12,        // SMS
            NotificationType::GenericChat => 12,       // SMS (chat uses same as SMS)
            NotificationType::GenericNavigation => 10, // LOCATION
            NotificationType::GenericSocial => 4,      // SOCIAL
            NotificationType::GenericCalendar => 5,    // SCHEDULE
            NotificationType::GenericAlarmClock => 0,  // OTHER
            NotificationType::Generic => 0,            // OTHER
        }
    }

    /// Get notification flags based on type
    fn notification_flags(&self, _has_actions: bool) -> u8 {
        let mut flags = 0u8;

        // Bit 0: BACKGROUND (0) / FOREGROUND (1)
        // Bit 1: FOREGROUND flag
        // Bit 3: ACTION_ACCEPT (for legacy actions)
        // Bit 4: ACTION_DECLINE

        // Most notifications are foreground
        flags |= 0x02; // FOREGROUND flag

        // ACTION_DECLINE is required for ALL notifications to make them alerting/dismissible
        // This matches the Java implementation which always sets this flag
        flags |= 0x10; // ACTION_DECLINE

        flags
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NotificationUpdateType {
    Add = 0,
    Modify = 1,
    Remove = 2,
}

pub struct NotificationUpdateMessageBuilder {
    update_type: NotificationUpdateType,
    notification_type: NotificationType,
    count: u8,
    notification_id: i32,
    has_actions: bool,
    has_picture: bool,
}

/// Generic notification specification
#[derive(Debug, Clone)]
pub struct NotificationSpec {
    pub id: i32,
    pub notification_type: NotificationType,
    pub title: Option<String>,
    pub body: Option<String>,
    pub sender: Option<String>,
    pub phone_number: Option<String>,
    pub source_name: Option<String>,
    pub when: u64,
    pub has_actions: bool,
    pub has_picture: bool,
    pub timestamp: std::time::Instant,
    pub retrieved: bool,
}

impl NotificationSpec {
    pub fn new(id: i32, notification_type: NotificationType) -> Self {
        Self {
            id,
            notification_type,
            title: None,
            body: None,
            sender: None,
            phone_number: None,
            source_name: None,
            when: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            has_actions: true, // Enable actions (dismiss) for all notifications
            has_picture: false,
            timestamp: std::time::Instant::now(),
            retrieved: false,
        }
    }

    pub fn with_title(mut self, title: String) -> Self {
        self.title = Some(title);
        self
    }

    pub fn with_body(mut self, body: String) -> Self {
        self.body = Some(body);
        self
    }

    pub fn with_sender(mut self, sender: String) -> Self {
        self.sender = Some(sender);
        self
    }

    pub fn with_phone_number(mut self, phone_number: String) -> Self {
        self.phone_number = Some(phone_number);
        self
    }

    pub fn with_source_name(mut self, source_name: String) -> Self {
        self.source_name = Some(source_name);
        self
    }

    pub fn with_actions(mut self, has_actions: bool) -> Self {
        self.has_actions = has_actions;
        self
    }

    pub fn with_picture(mut self, has_picture: bool) -> Self {
        self.has_picture = has_picture;
        self
    }
}

impl NotificationUpdateMessageBuilder {
    pub fn new(
        update_type: NotificationUpdateType,
        notification_type: NotificationType,
        count: u8,
        notification_id: i32,
    ) -> Self {
        Self {
            update_type,
            notification_type,
            count,
            notification_id,
            has_actions: false,
            has_picture: false,
        }
    }

    pub fn with_actions(mut self, has_actions: bool) -> Self {
        self.has_actions = has_actions;
        self
    }

    pub fn build(&self) -> Vec<u8> {
        let mut message = Vec::with_capacity(9);

        // NOTE: Message ID is added by wrap_in_gfdi_envelope(), not here
        // This is just the payload

        message.push(self.update_type as u8);

        // Category flags
        let category_flags = self.notification_type.notification_flags(self.has_actions);
        message.push(category_flags);

        // Category value
        message.push(self.notification_type.category_value());

        // Count of this notification type
        message.push(self.count);

        // Notification ID
        message.extend_from_slice(&self.notification_id.to_le_bytes());

        // Phone flags (actions and attachments)
        let mut phone_flags = 0u8;
        if self.has_actions {
            phone_flags |= 0x02; // NEW_ACTIONS flag
        }
        if self.has_picture {
            phone_flags |= 0x04; // HAS_ATTACHMENTS flag
        }
        message.push(phone_flags);

        message
    }
}

pub struct GarminNotificationHandler {
    communicator: Arc<futures::lock::Mutex<Arc<CommunicatorV2>>>,
    active_notifications: Arc<Mutex<HashMap<i32, CallSpec>>>,
    notification_counts: Arc<Mutex<HashMap<NotificationType, u8>>>,
    stored_notifications: Arc<Mutex<HashMap<i32, NotificationSpec>>>,
    #[allow(dead_code)]
    last_control_request: Arc<Mutex<Option<(i32, Vec<u8>)>>>, // (notification_id, attribut...
    message_size_request_count: Arc<Mutex<HashMap<i32, u8>>>, // Track MESSAGE_SIZE-only request count per notification
    next_notification_id: Arc<Mutex<i32>>,
    last_cleanup_time: Arc<Mutex<std::time::Instant>>,
    seen_dbus_messages: Arc<Mutex<std::collections::HashSet<(String, u64)>>>, // (summary, body_hash) - app_name excluded to handle same notification from different interfaces
    watchdog: Arc<WatchdogManager>,
    missed_notifications: Arc<Mutex<VecDeque<NotificationSpec>>>,
    is_connected: Arc<Mutex<bool>>,
}

impl GarminNotificationHandler {
    pub fn new(communicator: Arc<CommunicatorV2>, watchdog: Arc<WatchdogManager>) -> Self {
        Self {
            communicator: Arc::new(futures::lock::Mutex::new(communicator)),
            active_notifications: Arc::new(Mutex::new(HashMap::new())),
            notification_counts: Arc::new(Mutex::new(HashMap::new())),
            stored_notifications: Arc::new(Mutex::new(HashMap::new())),
            last_control_request: Arc::new(Mutex::new(None)),
            message_size_request_count: Arc::new(Mutex::new(HashMap::new())),
            next_notification_id: Arc::new(Mutex::new(100)),
            last_cleanup_time: Arc::new(Mutex::new(std::time::Instant::now())),
            seen_dbus_messages: Arc::new(Mutex::new(std::collections::HashSet::new())),
            watchdog,
            missed_notifications: Arc::new(Mutex::new(VecDeque::new())),
            is_connected: Arc::new(Mutex::new(true)),
        }
    }

    pub fn get_next_notification_id(&self) -> i32 {
        let mut id = self.next_notification_id.lock().unwrap();
        let current = *id;
        *id += 1;
        current
    }

    /// Clean up old notifications that were never retrieved by the watch
    /// This helps prevent notification count from growing too large
    pub async fn cleanup_old_notifications(&self) {
        // Check if cleanup is needed and update timestamp
        let should_cleanup = {
            let mut last_cleanup = self.last_cleanup_time.lock().unwrap();

            // Only cleanup every 2 minutes
            if last_cleanup.elapsed() < std::time::Duration::from_secs(120) {
                false
            } else {
                *last_cleanup = std::time::Instant::now();
                true
            }
        }; // MutexGuard dropped here

        if !should_cleanup {
            return;
        }

        // Find notifications that are old and unretrieved
        let notifications_to_remove: Vec<(i32, NotificationType)> = {
            let stored = self.stored_notifications.lock().unwrap();
            let now = std::time::Instant::now();

            stored
                .iter()
                .filter(|(_, spec)| {
                    // Remove if older than 10 minutes and not retrieved
                    !spec.retrieved
                        && now.duration_since(spec.timestamp) > std::time::Duration::from_secs(600)
                })
                .map(|(id, spec)| (*id, spec.notification_type))
                .collect()
        };

        if !notifications_to_remove.is_empty() {
            // Send remove commands to watch for each notification
            for (id, _notif_type) in notifications_to_remove {
                if let Err(e) = self.remove_notification(id).await {
                    error!("   ⚠️  Failed to remove notification {}: {}", id, e);
                }
            }
        }

        // If we still have more than 30 stored notifications, clear old ones regardless
        let stored_count = {
            let stored = self.stored_notifications.lock().unwrap();
            stored.len()
        };

        if stored_count > 30 {
            error!(
                "🧹 Too many notifications ({}), clearing all and resetting counts",
                stored_count
            );

            let all_notifications: Vec<(i32, NotificationType)> = {
                let stored = self.stored_notifications.lock().unwrap();
                stored
                    .iter()
                    .map(|(id, spec)| (*id, spec.notification_type))
                    .collect()
            };

            for (id, _notif_type) in all_notifications {
                if let Err(e) = self.remove_notification(id).await {
                    error!("   ⚠️  Failed to remove notification {}: {}", id, e);
                }
            }

            // Note: Counts are now calculated dynamically, no need to reset manually
        }
    }

    /// Remove a notification from the watch
    pub async fn remove_notification(&self, notification_id: i32) -> types::Result<()> {
        debug!(
            "🗑️  Removing notification ID {} from watch",
            notification_id
        );

        // Get notification type before removing
        let notification_type = {
            let stored = self.stored_notifications.lock().unwrap();
            stored
                .get(&notification_id)
                .map(|spec| spec.notification_type)
                .unwrap_or(NotificationType::Generic)
        };

        // Remove from stored notifications
        {
            let mut stored = self.stored_notifications.lock().unwrap();
            stored.remove(&notification_id);
        }

        // Send remove update to watch
        let remove_msg = NotificationUpdateMessageBuilder::new(
            NotificationUpdateType::Remove,
            notification_type,
            0, // count is 0 for remove
            notification_id,
        )
        .build();

        let gfdi_message = self.wrap_in_gfdi_envelope(5033, &remove_msg);
        self.communicator
            .lock()
            .await
            .send_message("notification_remove", &gfdi_message)
            .await?;

        Ok(())
    }

    /// Send a generic notification (SMS, email, chat, etc.)
    pub async fn on_notification(&self, notification: NotificationSpec) -> types::Result<()> {
        // Check if connected
        let is_connected = *self.is_connected.lock().unwrap();

        if !is_connected {
            // Store for later replay
            info!(
                "⏸️  Watch disconnected - queueing notification {} for later",
                notification.id
            );
            self.missed_notifications
                .lock()
                .unwrap()
                .push_back(notification);
            return Ok(());
        }

        // Cleanup old notifications periodically
        self.cleanup_old_notifications().await;
        info!(
            "📨 Sending {} notification (ID: {})",
            match notification.notification_type {
                NotificationType::GenericPhone => "Phone",
                NotificationType::GenericSms => "SMS",
                NotificationType::GenericEmail => "Email",
                NotificationType::GenericChat => "Chat",
                NotificationType::GenericSocial => "Social",
                NotificationType::GenericNavigation => "Navigation",
                NotificationType::GenericCalendar => "Calendar",
                NotificationType::GenericAlarmClock => "Alarm",
                NotificationType::Generic => "Generic",
            },
            notification.id
        );

        // Calculate actual count of unretrieved notifications of this type
        // This gives the watch an accurate count instead of an ever-increasing number
        let (count, unretrieved_ids) = {
            let stored = self.stored_notifications.lock().unwrap();
            let unretrieved: Vec<(i32, NotificationType)> = stored
                .iter()
                .filter(|(_, spec)| {
                    spec.notification_type == notification.notification_type && !spec.retrieved
                })
                .map(|(id, spec)| (*id, spec.notification_type))
                .collect();
            let unretrieved_count = unretrieved.len() as u8;
            // Add 1 for the notification we're about to add
            ((unretrieved_count + 1).min(50), unretrieved)
        };

        // If count is >= 5, clean up old unretrieved notifications
        // BUT only remove notifications that are at least 60 seconds old
        // The watch can take 30-60 seconds to request notification attributes
        if count >= 5 {
            error!(
                "   🧹 Count too high ({}), removing OLD unretrieved notifications (>60s)",
                count
            );
            let now = std::time::Instant::now();
            let mut removed_count = 0;

            // Only remove notifications that are both unretrieved AND old enough
            let total_unretrieved = unretrieved_ids.len();
            for (id, _notif_type) in &unretrieved_ids {
                // Check age of this notification
                let is_old = {
                    let stored = self.stored_notifications.lock().unwrap();
                    if let Some(spec) = stored.get(&id) {
                        now.duration_since(spec.timestamp) > std::time::Duration::from_secs(60)
                    } else {
                        false // Already removed, skip
                    }
                };

                if is_old {
                    if let Err(e) = self.remove_notification(*id).await {
                        error!("   ⚠️  Failed to remove notification {}: {}", id, e);
                    } else {
                        removed_count += 1;
                    }
                } else {
                    info!(
                        "   ⏳ Keeping notification {} (too recent, watch may still request it)",
                        id
                    );
                }
            }

            if removed_count > 0 {
                info!(
                    "   ✅ Removed {} old notifications, {} recent ones kept",
                    removed_count,
                    total_unretrieved - removed_count
                );
            } else {
                debug!("   ℹ️  No old notifications to remove, all are recent");
            }
            let count = 1;

            let update_msg = NotificationUpdateMessageBuilder::new(
                NotificationUpdateType::Add,
                notification.notification_type,
                count,
                notification.id,
            )
            .with_actions(notification.has_actions)
            .build();

            let gfdi_message = self.wrap_in_gfdi_envelope(5033, &update_msg);

            // Try to send - detect "Not connected" errors
            match self
                .communicator
                .lock()
                .await
                .send_message("notification", &gfdi_message)
                .await
            {
                Ok(_) => {
                    debug!("   ✅ Notification sent successfully");
                }
                Err(e) => {
                    let err_msg = format!("{}", e);
                    if err_msg.contains("Not connected") || err_msg.contains("not connected") {
                        debug!("❌ BLE connection lost - triggering immediate watchdog restart");
                        // Mark as disconnected so watchdog triggers immediately
                        let watchdog = self.watchdog.clone();
                        tokio::task::block_in_place(|| {
                            tokio::runtime::Handle::current()
                                .block_on(watchdog.mark_disconnected());
                        });
                    }
                    return Err(e);
                }
            }

            // Store the notification for later retrieval when watch requests details
            let notification_id = notification.id;
            {
                let mut stored = self.stored_notifications.lock().unwrap();
                stored.insert(notification_id, notification);
            }

            return Ok(());
        }

        let update_msg = NotificationUpdateMessageBuilder::new(
            NotificationUpdateType::Add,
            notification.notification_type,
            count,
            notification.id,
        )
        .with_actions(notification.has_actions)
        .build();

        let gfdi_message = self.wrap_in_gfdi_envelope(5033, &update_msg);

        // Try to send - detect "Not connected" errors
        match self
            .communicator
            .lock()
            .await
            .send_message("notification", &gfdi_message)
            .await
        {
            Ok(_) => {
                info!("   ✅ Notification sent successfully");
            }
            Err(e) => {
                let err_msg = format!("{}", e);
                if err_msg.contains("Not connected") || err_msg.contains("not connected") {
                    debug!("❌ BLE connection lost during notification send");

                    // Mark handler and watchdog as disconnected
                    self.set_connected(false);
                    let watchdog = self.watchdog.clone();
                    tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(watchdog.mark_disconnected());
                    });

                    // Queue this notification for replay after reconnection
                    info!(
                        "⏸️  Queueing notification {} for replay after reconnect",
                        notification.id
                    );
                    self.missed_notifications
                        .lock()
                        .unwrap()
                        .push_back(notification);

                    return Ok(()); // Don't return error - we've queued it
                }
                return Err(e);
            }
        }

        // Store the notification for later retrieval when watch requests details
        let notification_id = notification.id;
        {
            let mut stored = self.stored_notifications.lock().unwrap();
            stored.insert(notification_id, notification);
        }

        Ok(())
    }

    /// Replay all missed notifications
    pub async fn replay_missed_notifications(&self) -> types::Result<()> {
        let notifications: Vec<NotificationSpec> = {
            let mut missed = self.missed_notifications.lock().unwrap();
            let notifications: Vec<_> = missed.drain(..).collect();
            notifications
        };

        if notifications.is_empty() {
            return Ok(());
        }

        info!(
            "\n🔄 Replaying {} missed notifications...",
            notifications.len()
        );

        for notif in notifications {
            if let Err(e) = self.on_notification(notif).await {
                error!("   ⚠️  Failed to replay notification: {}", e);
            }
            // Small delay between notifications
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        debug!("   ✅ Finished replaying missed notifications\n");
        Ok(())
    }

    async fn set_communicator(&self, comm: Arc<CommunicatorV2>) {
        *self.communicator.lock().await = comm;
    }

    /// Mark handler as connected or disconnected
    pub fn set_connected(&self, connected: bool) {
        *self.is_connected.lock().unwrap() = connected;
    }

    /// Handle NotificationControl request from watch
    pub async fn on_notification_control(
        &self,
        notification_id: i32,
        command: u8,
        attributes: &[(u8, u16)],
    ) -> types::Result<()> {
        // Command 0 = GET_NOTIFICATION_ATTRIBUTES
        if command == 0 {
            // Continue with attribute retrieval (existing code below)
        }
        // Command 2 = PERFORM_LEGACY_NOTIFICATION_ACTION (legacy dismiss/action)
        else if command == 2 {
            // Remove notification from our cache
            {
                let mut stored = self.stored_notifications.lock().unwrap();
                if stored.remove(&notification_id).is_some() {
                    debug!("   ✅ Notification {} removed from cache", notification_id);
                } else {
                    error!("   ⚠️  Notification {} not in cache", notification_id);
                }
            }

            // Send acknowledgment back to watch
            let response_payload = vec![
                0xAA, 0x13, // Original message ID: NotificationControl (5034)
                0x00, // Status: ACK
            ];

            let response = self.wrap_in_gfdi_envelope(0x1388, &response_payload);
            self.communicator
                .lock()
                .await
                .send_message("notification_action_ack", &response)
                .await?;

            return Ok(());
        }
        // Command 128 = PERFORM_NOTIFICATION_ACTION (modern action with action_id)
        else if command == 128 {
            println!("   🗑️  PERFORM_NOTIFICATION_ACTION - modern action");

            // Log action details from parsed message
            if let Some(action_id) = attributes.first().and_then(|_| {
                // The action_id should be in the struct fields, but we're getting it via attributes temporarily
                // This is a workaround - ideally we'd access msg.action_id directly
                None::<u8>
            }) {
                let action_name = match action_id {
                    1..=5 => format!("CUSTOM_ACTION_{}", action_id),
                    94 => "REPLY_INCOMING_CALL".to_string(),
                    95 => "REPLY_MESSAGES".to_string(),
                    96 => "ACCEPT_INCOMING_CALL".to_string(),
                    97 => "REJECT_INCOMING_CALL".to_string(),
                    98 => "DISMISS_NOTIFICATION".to_string(),
                    99 => "BLOCK_APPLICATION".to_string(),
                    _ => format!("UNKNOWN({})", action_id),
                };
                println!("   📋 Action: {} (id={})", action_name, action_id);
            }

            // Get notification type before removing from cache
            let notification_type = {
                let stored = self.stored_notifications.lock().unwrap();
                stored.get(&notification_id).map(|n| n.notification_type)
            };

            // Send NotificationUpdate Remove to watch to properly dismiss
            if let Some(notif_type) = notification_type {
                println!("   📤 Sending NotificationUpdate Remove to watch");
                let remove_msg = NotificationUpdateMessageBuilder::new(
                    NotificationUpdateType::Remove,
                    notif_type,
                    0, // count = 0 for remove
                    notification_id,
                )
                .build();

                let gfdi_message = self.wrap_in_gfdi_envelope(5033, &remove_msg);
                if let Err(e) = self
                    .communicator
                    .lock()
                    .await
                    .send_message("notification_remove", &gfdi_message)
                    .await
                {
                    eprintln!("   ⚠️  Failed to send remove notification: {}", e);
                }
            }

            // Remove notification from our cache
            {
                let mut stored = self.stored_notifications.lock().unwrap();
                if stored.remove(&notification_id).is_some() {
                    println!("   ✅ Notification {} removed from cache", notification_id);
                } else {
                    println!("   ⚠️  Notification {} not in cache", notification_id);
                }
            }

            // Send acknowledgment back to watch
            let response_payload = vec![
                0xAA, 0x13, // Original message ID: NotificationControl (5034)
                0x00, // Status: ACK
            ];

            let response = self.wrap_in_gfdi_envelope(0x1388, &response_payload);
            self.communicator
                .lock()
                .await
                .send_message("notification_action_ack", &response)
                .await?;

            println!("   ✅ Action ACK sent to watch");
            return Ok(());
        } else {
            println!("   ⚠️  Unknown command: {}", command);
            return Ok(());
        }

        // Check if this is a MESSAGE_SIZE-only request
        // The watch may request MESSAGE_SIZE multiple times to verify the data
        // However, if it keeps looping, we need to stop and investigate
        let is_message_size_only = attributes.len() == 1 && attributes[0].0 == 4;

        if is_message_size_only {
            let mut count_map = self.message_size_request_count.lock().unwrap();
            let count = count_map.entry(notification_id).or_insert(0);
            *count += 1;

            println!("   🔍 MESSAGE_SIZE-only request #{}", count);
            println!("      Requested max_len: {}", attributes[0].1);

            if *count > 10 {
                println!(
                    "   ⚠️  MESSAGE_SIZE requested {} times - this is abnormal!",
                    count
                );
                println!("   💡 The watch may be expecting a different response format");
                println!("   💡 Possible issues:");
                println!("      - CRC mismatch in notification data");
                println!("      - Missing status acknowledgment");
                println!("      - Incorrect chunking protocol");
                return Ok(());
            }
        } else {
            // Reset counter for non-MESSAGE_SIZE-only requests
            let mut count_map = self.message_size_request_count.lock().unwrap();
            count_map.remove(&notification_id);
            println!("   📋 Full attribute request - resetting MESSAGE_SIZE counter");

            // Mark this notification as retrieved (watch has requested it)
            {
                let mut stored = self.stored_notifications.lock().unwrap();
                if let Some(spec) = stored.get_mut(&notification_id) {
                    spec.retrieved = true;
                    println!("   ✅ Marked notification {} as retrieved", notification_id);
                }
            }
        }

        // Log detailed attribute information
        println!("   📝 Notification attributes requested:");
        for (attr_id, max_len) in attributes {
            let attr_name = match attr_id {
                0 => "APP_IDENTIFIER",
                1 => "TITLE",
                2 => "SUBTITLE",
                3 => "MESSAGE",
                4 => "MESSAGE_SIZE",
                5 => "DATE",
                9 => "SENDER",
                127 => "ACTIONS",
                _ => "UNKNOWN",
            };
            println!(
                "      - {} (id={}, max_len={})",
                attr_name, attr_id, max_len
            );
        }

        // Retrieve the stored notification
        let notification = {
            let stored = self.stored_notifications.lock().unwrap();
            stored.get(&notification_id).cloned()
        };

        match notification {
            Some(notif) => {
                println!("   ✅ Found notification in cache");
                println!("      📋 Notification details:");
                println!("         Type: {:?}", notif.notification_type);
                println!("         Title: {:?}", notif.title.as_deref().unwrap_or(""));
                println!("         Body: {:?}", notif.body.as_deref().unwrap_or(""));
                println!(
                    "         Body length: {} bytes",
                    notif.body.as_deref().unwrap_or("").len()
                );
                println!(
                    "         Sender: {:?}",
                    notif.sender.as_deref().unwrap_or("")
                );

                // Get current timestamp in format yyyyMMddTHHmmss
                use std::time::{SystemTime, UNIX_EPOCH};
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let timestamp = chrono::DateTime::<chrono::Utc>::from_timestamp(now as i64, 0)
                    .unwrap()
                    .format("%Y%m%dT%H%M%S")
                    .to_string();

                println!("      📅 Timestamp: {}", timestamp);

                // Log what we're about to encode
                println!("      🔧 Encoding attributes:");
                for (attr_id, max_len) in attributes {
                    let attr_name = match attr_id {
                        0 => "APP_IDENTIFIER",
                        1 => "TITLE",
                        2 => "SUBTITLE",
                        3 => "MESSAGE",
                        4 => "MESSAGE_SIZE",
                        5 => "DATE",
                        9 => "SENDER",
                        127 => "ACTIONS",
                        _ => "UNKNOWN",
                    };
                    let value = match attr_id {
                        0 => "com.gadgetbridge",
                        1 => {
                            // For SMS, TITLE should be sender (matching Java)
                            if matches!(notif.notification_type, NotificationType::GenericSms) {
                                notif.sender.as_deref().unwrap_or("")
                            } else {
                                notif.title.as_deref().unwrap_or("")
                            }
                        }
                        2 => "", // SUBTITLE - not used for notifications
                        3 => notif.body.as_deref().unwrap_or(""),
                        4 => &notif.body.as_deref().unwrap_or("").len().to_string(),
                        5 => &timestamp, // DATE
                        9 => notif.sender.as_deref().unwrap_or(""),
                        127 => "[DISMISS action]", // ACTIONS
                        _ => "",
                    };
                    let truncated = if *max_len > 0 && *max_len < 0xFFFF {
                        &value[..std::cmp::min(value.len(), *max_len as usize)]
                    } else {
                        value
                    };
                    println!(
                        "         - {}: \"{}\" (len={}, max_len={})",
                        attr_name,
                        truncated,
                        truncated.len(),
                        max_len
                    );
                }

                // Generate NotificationData message
                // For SMS, use sender as TITLE (matching Java behavior)
                let title_to_send =
                    if matches!(notif.notification_type, NotificationType::GenericSms) {
                        notif.sender.as_deref().unwrap_or("")
                    } else {
                        notif.title.as_deref().unwrap_or("")
                    };

                let data_msg = MessageGenerator::notification_data(
                    notification_id as u32,
                    attributes,
                    title_to_send,
                    notif.body.as_deref().unwrap_or(""),
                    notif.sender.as_deref().unwrap_or(""),
                    &timestamp,
                    "com.gadgetbridge",
                )?;

                println!("   📤 Sending NotificationData ({} bytes)", data_msg.len());

                // Log the hex of the message for debugging
                if data_msg.len() <= 100 {
                    println!(
                        "      Hex: {}",
                        data_msg
                            .iter()
                            .map(|b| format!("{:02X}", b))
                            .collect::<Vec<_>>()
                            .join(" ")
                    );
                } else {
                    println!(
                        "      First 50 bytes: {}",
                        data_msg
                            .iter()
                            .take(50)
                            .map(|b| format!("{:02X}", b))
                            .collect::<Vec<_>>()
                            .join(" ")
                    );
                }

                // Try to send - detect "Not connected" errors
                match self
                    .communicator
                    .lock()
                    .await
                    .send_message("notification_data", &data_msg)
                    .await
                {
                    Ok(_) => {
                        // TX watchdog removed - only monitoring RX traffic
                    }
                    Err(e) => {
                        let err_msg = format!("{}", e);
                        if err_msg.contains("Not connected") || err_msg.contains("not connected") {
                            eprintln!(
                                "❌ BLE connection lost - triggering immediate watchdog restart"
                            );
                            // Mark as disconnected so watchdog triggers immediately
                            let watchdog = self.watchdog.clone();
                            tokio::task::block_in_place(|| {
                                tokio::runtime::Handle::current()
                                    .block_on(watchdog.mark_disconnected());
                            });
                        }
                        return Err(e);
                    }
                }

                println!("   ✅ NotificationData sent successfully");
                println!("      ⏳ Waiting for watch Response (0x1388) with ACK status...");
            }
            None => {
                eprintln!("   ❌ Notification {} not found in cache", notification_id);
            }
        }

        Ok(())
    }

    /// Delete a notification by ID and type
    pub async fn delete_notification(
        &self,
        id: i32,
        notification_type: NotificationType,
    ) -> types::Result<()> {
        println!("🗑️  Deleting notification (ID: {})", id);

        // Update notification count for this type
        let count = {
            let mut counts = self.notification_counts.lock().unwrap();
            let count = counts.entry(notification_type).or_insert(1);
            if *count > 0 {
                *count -= 1;
            }
            *count
        };

        let remove_msg = NotificationUpdateMessageBuilder::new(
            NotificationUpdateType::Remove,
            notification_type,
            count,
            id,
        )
        .build();

        let gfdi_message = self.wrap_in_gfdi_envelope(5033, &remove_msg);

        // Try to send - detect "Not connected" errors
        match self
            .communicator
            .lock()
            .await
            .send_message("notification", &gfdi_message)
            .await
        {
            Ok(_) => {
                // TX watchdog removed - only monitoring RX traffic
            }
            Err(e) => {
                let err_msg = format!("{}", e);
                if err_msg.contains("Not connected") || err_msg.contains("not connected") {
                    eprintln!("❌ BLE connection lost - triggering immediate watchdog restart");
                    // Mark as disconnected so watchdog triggers immediately
                    let watchdog = self.watchdog.clone();
                    tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(watchdog.mark_disconnected());
                    });
                }
                return Err(e);
            }
        }

        Ok(())
    }

    pub async fn on_set_call_state(&self, call_spec: CallSpec) -> types::Result<()> {
        let id = call_spec.get_id();

        match call_spec.command {
            CallCommand::Incoming => {
                println!("📞 Incoming call from: {}", call_spec.number);
                if let Some(ref name) = call_spec.name {
                    println!("   Caller: {}", name);
                }

                self.active_notifications
                    .lock()
                    .unwrap()
                    .insert(id, call_spec.clone());

                // Also store as NotificationSpec so watch can retrieve details
                let notification_spec = NotificationSpec::new(id, NotificationType::GenericPhone)
                    .with_title(
                        call_spec
                            .name
                            .clone()
                            .unwrap_or_else(|| "Incoming Call".to_string()),
                    )
                    .with_body(format!("From: {}", call_spec.number))
                    .with_sender(
                        call_spec
                            .source_name
                            .clone()
                            .unwrap_or_else(|| "Phone".to_string()),
                    )
                    .with_phone_number(call_spec.number.clone());

                self.stored_notifications
                    .lock()
                    .unwrap()
                    .insert(id, notification_spec);

                let update_msg = NotificationUpdateMessageBuilder::new(
                    NotificationUpdateType::Add,
                    NotificationType::GenericPhone,
                    1,
                    id,
                )
                .with_actions(true)
                .build();

                let gfdi_message = self.wrap_in_gfdi_envelope(5033, &update_msg);

                // Try to send - detect "Not connected" errors
                match self
                    .communicator
                    .lock()
                    .await
                    .send_message("notification", &gfdi_message)
                    .await
                {
                    Ok(_) => {
                        // TX watchdog removed - only monitoring RX traffic
                    }
                    Err(e) => {
                        let err_msg = format!("{}", e);
                        if err_msg.contains("Not connected") || err_msg.contains("not connected") {
                            eprintln!(
                                "❌ BLE connection lost - triggering immediate watchdog restart"
                            );
                            // Mark as disconnected so watchdog triggers immediately
                            let watchdog = self.watchdog.clone();
                            tokio::task::block_in_place(|| {
                                tokio::runtime::Handle::current()
                                    .block_on(watchdog.mark_disconnected());
                            });
                        }
                        return Err(e);
                    }
                }

                println!("✅ Incoming call notification sent (ID: {})", id);
            }

            CallCommand::End | CallCommand::Reject => {
                println!("❌ Call ended/rejected");
                self.active_notifications.lock().unwrap().remove(&id);
                self.stored_notifications.lock().unwrap().remove(&id);

                let remove_msg = NotificationUpdateMessageBuilder::new(
                    NotificationUpdateType::Remove,
                    NotificationType::GenericPhone,
                    0,
                    id,
                )
                .build();

                let gfdi_message = self.wrap_in_gfdi_envelope(5033, &remove_msg);
                self.communicator
                    .lock()
                    .await
                    .send_message("call_end_notification", &gfdi_message)
                    .await?;

                println!("✅ Call end notification sent (ID: {})", id);
            }

            _ => {}
        }

        Ok(())
    }

    fn wrap_in_gfdi_envelope(&self, message_id: u16, payload: &[u8]) -> Vec<u8> {
        let mut message = Vec::new();

        // Calculate packet size - TOTAL packet size including size field itself
        // Java: payloadSize must equal byteBuffer.capacity() (total packet size)
        // Total = size field (2) + message_id (2) + payload (N) + CRC (2)
        let packet_size = (2 + 2 + payload.len() + 2) as u16;
        message.extend_from_slice(&packet_size.to_le_bytes());

        // Message ID (little-endian)
        message.extend_from_slice(&message_id.to_le_bytes());

        // Payload
        message.extend_from_slice(payload);

        // Calculate CRC over size + message_id + payload (everything so far)
        let crc = self.compute_crc(&message);
        message.extend_from_slice(&crc.to_le_bytes());

        message
    }

    fn compute_crc(&self, data: &[u8]) -> u16 {
        // CRC lookup table from Java ChecksumCalculator
        const CONSTANTS: [u16; 16] = [
            0x0000, 0xCC01, 0xD801, 0x1400, 0xF001, 0x3C00, 0x2800, 0xE401, 0xA001, 0x6C00, 0x7800,
            0xB401, 0x5000, 0x9C01, 0x8801, 0x4400,
        ];

        let mut crc: u16 = 0;
        for &byte in data {
            let b = byte as u16;
            // Process low nibble
            crc = (((crc >> 4) & 0x0FFF) ^ CONSTANTS[(crc & 0x0F) as usize])
                ^ CONSTANTS[(b & 0x0F) as usize];
            // Process high nibble
            crc = (((crc >> 4) & 0x0FFF) ^ CONSTANTS[(crc & 0x0F) as usize])
                ^ CONSTANTS[((b >> 4) & 0x0F) as usize];
        }
        crc
    }
}

// ============================================================================
// Real Bluetooth Implementation using BlueR
// ============================================================================

struct BlueRSupport {
    device: Arc<Device>,
    characteristics: Arc<Mutex<HashMap<String, Characteristic>>>,
    listener_shutdown: Arc<Mutex<Option<tokio::sync::mpsc::Sender<()>>>>,
}

impl BlueRSupport {
    async fn new(
        adapter: &Adapter,
        mac_address: &str,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        println!("🔍 Searching for device {}...", mac_address);

        // Parse MAC address
        let address: Address = mac_address.parse().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid MAC address: {}", mac_address),
            )
        })?;

        // Get device
        let device = adapter.device(address)?;

        println!("📡 Connecting to device...");
        if !device.is_connected().await? {
            device.connect().await?;
            println!("✅ Connected!");
        } else {
            println!("✅ Already connected!");
        }

        // Wait a moment for connection to stabilize
        sleep(Duration::from_millis(500)).await;

        Ok(Self {
            device: Arc::new(device),
            characteristics: Arc::new(Mutex::new(HashMap::new())),
            listener_shutdown: Arc::new(Mutex::new(None)),
        })
    }

    async fn discover_services(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        println!("🔍 Discovering services...");

        // Wait for GATT services to be resolved
        println!("   ⏳ Waiting for GATT services to be resolved...");
        let mut attempts = 0;
        const MAX_ATTEMPTS: u32 = 30; // 30 seconds max

        loop {
            match self.device.is_services_resolved().await {
                Ok(true) => {
                    println!("   ✅ GATT services resolved");
                    break;
                }
                Ok(false) => {
                    attempts += 1;
                    if attempts >= MAX_ATTEMPTS {
                        return Err("Timeout waiting for GATT services to be resolved".into());
                    }
                    sleep(Duration::from_secs(1)).await;
                }
                Err(e) => {
                    println!("   ⚠️  Could not check services resolved status: {}", e);
                    // Continue anyway after a short delay
                    sleep(Duration::from_secs(2)).await;
                    break;
                }
            }
        }

        // Get all services
        let services = self.device.services().await?;
        println!("   Found {} services", services.len());

        for service in services {
            let _uuid = service.uuid().await?;
            let characteristics = service.characteristics().await?;

            for characteristic in characteristics {
                let char_uuid = characteristic.uuid().await?;
                let uuid_str = char_uuid.to_string().to_uppercase();

                // Store characteristic
                self.characteristics
                    .lock()
                    .unwrap()
                    .insert(uuid_str.clone(), characteristic);
            }
        }

        println!(
            "   Found {} characteristics",
            self.characteristics.lock().unwrap().len()
        );
        Ok(())
    }

    /// Start notification listener with real-time bidirectional communication
    async fn start_notification_listener(
        &self,
        uuid: &str,
        communicator: Arc<CommunicatorV2>,
        watchdog: Option<Arc<WatchdogManager>>,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        // Create shutdown channel
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

        // Store shutdown sender so we can stop the listener later
        *self.listener_shutdown.lock().unwrap() = Some(shutdown_tx);
        println!(
            "📡 Starting real-time notification listener on {}",
            &uuid[..13]
        );

        let mut uuid_copy = String::from("");
        uuid_copy.push_str(uuid);
        let characteristics = self.characteristics.lock().unwrap().clone();

        // Create a channel to signal when listener is ready
        let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(1);

        tokio::spawn(async move {
            let uuid_upper = uuid_copy.to_uppercase();

            if let Some(characteristic) = characteristics.get(&uuid_upper) {
                match characteristic.notify().await {
                    Ok(stream) => {
                        info!("   ✅ Notification stream active - bidirectional communication enabled");

                        // Signal that we're ready
                        let _ = ready_tx.send(()).await;

                        // Pin the stream to make it work with StreamExt
                        let mut stream = Box::pin(stream);

                        loop {
                            tokio::select! {
                                Some(value) = stream.next() => {
                            debug!("📥 RAW DATA FROM WATCH:");
                            debug!("   Length: {} bytes", value.len());
                            debug!("   Hex: {}", hex_dump(&value, 32));

                            // Record RX traffic for watchdog
                            if let Some(ref wd) = watchdog {
                                wd.record_rx().await;
                            }

                            // Process received data through communicator
                                    if let Err(e) = communicator.on_characteristic_changed(&value).await {
                                        error!("   ❌ Error processing notification: {}", e);
                                    }
                                }
                                _ = shutdown_rx.recv() => {
                                    info!("   🛑 Listener shutdown requested");
                                    break;
                                }
                            }
                        }

                        info!("   ⚠️  Notification stream ended");
                    }
                    Err(e) => {
                        error!("   ❌ Failed to start notification stream: {}", e);
                    }
                }
            } else {
                error!("Characteristic {} not found", uuid_upper);
            }
        });

        // Wait for the listener to be ready
        match tokio::time::timeout(std::time::Duration::from_secs(5), ready_rx.recv()).await {
            Ok(Some(_)) => {
                info!("   ✅ Listener confirmed active and ready");
                Ok(())
            }
            Ok(None) => Err("Listener channel closed unexpectedly".into()),
            Err(_) => Err("Timeout waiting for listener to become active".into()),
        }
    }

    /// Stop the notification listener
    fn stop_listener(&self) {
        if let Some(tx) = self.listener_shutdown.lock().unwrap().take() {
            let _ = tx.try_send(());
            info!("   🛑 Sent shutdown signal to listener");
        }
    }
}

#[async_trait::async_trait]
impl BleSupport for BlueRSupport {
    fn get_characteristic(&self, uuid: &str) -> Option<CharacteristicHandle> {
        let uuid_upper = uuid.to_uppercase();
        if self
            .characteristics
            .lock()
            .unwrap()
            .contains_key(&uuid_upper)
        {
            Some(CharacteristicHandle::new(uuid.to_string()))
        } else {
            None
        }
    }

    fn enable_notifications(&self, _handle: &CharacteristicHandle) -> types::Result<()> {
        // Notifications will be enabled asynchronously
        Ok(())
    }

    async fn write_characteristic(
        &self,
        handle: &CharacteristicHandle,
        data: &[u8],
    ) -> types::Result<()> {
        debug!(
            "📤 BLE WRITE: {} bytes to characteristic {}",
            data.len(),
            &handle.uuid[..20]
        );
        debug!("   Hex: {}", hex_dump(data, 64));

        let uuid_upper = handle.uuid.to_uppercase();

        // Clone the characteristic from the HashMap
        let characteristic = {
            let characteristics = self.characteristics.lock().unwrap();
            characteristics.get(&uuid_upper).cloned()
        };

        if let Some(characteristic) = characteristic {
            let _result = characteristic.write(&data).await.map_err(|e| {
                error!("   ❌ BLE write failed: {}", e);
                crate::GarminError::BluetoothError(format!("Failed to write: {}", e))
            })?;
            debug!("   ✅ BLE write completed successfully");
            Ok(())
        } else {
            error!("   ❌ Characteristic {} not found", handle.uuid);
            Err(crate::GarminError::BluetoothError(format!(
                "Characteristic {} not found",
                handle.uuid
            )))
        }
    }

    fn create_transaction(&self, _name: &str) -> Box<dyn crate::Transaction> {
        Box::new(BlueRTransaction)
    }
}

struct BlueRTransaction;

impl Transaction for BlueRTransaction {
    fn write(&mut self, _handle: &CharacteristicHandle, _data: &[u8]) {
        // Transaction not implemented for real BLE
    }

    fn notify(&mut self, _handle: &CharacteristicHandle, _enable: bool) {
        // Transaction not implemented for real BLE
    }

    fn queue(self: Box<Self>) -> types::Result<()> {
        Ok(())
    }
}

/// Async message handler that processes incoming messages and generates responses
/// Pending protobuf chunk information
#[derive(Clone)]
struct PendingProtobufChunk {
    /// Complete protobuf payload
    complete_payload: Vec<u8>,
    /// Total length of the protobuf data
    total_length: usize,
    /// Original message type (0x13B4 for ProtobufResponse)
    #[allow(dead_code)]
    message_type: u16,
    /// Request ID
    #[allow(dead_code)]
    request_id: u16,
}

struct AsyncMessageHandler {
    communicator: Arc<Mutex<Option<Arc<CommunicatorV2>>>>,
    initialization_complete: Arc<Mutex<bool>>,
    notification_handler: Arc<Mutex<Option<Arc<GarminNotificationHandler>>>>,
    pairing_detected: Arc<Mutex<bool>>,
    message_queue: Arc<Mutex<VecDeque<Vec<u8>>>>,
    send_semaphore: Arc<Semaphore>,
    watchdog: Arc<Mutex<Option<Arc<WatchdogManager>>>>,
    data_transfer_handler: Arc<DataTransferHandler>,
    calendar_manager: Arc<Mutex<Option<Arc<CalendarManager>>>>,
    /// Map of request_id -> pending protobuf chunks that need to be sent incrementally
    pending_protobuf_chunks: Arc<Mutex<HashMap<u16, PendingProtobufChunk>>>,
    weather_provider: Arc<UnifiedWeatherProvider>,
}

/// Maximum protobuf data size per PROTOBUF_RESPONSE chunk (tested on Garmin devices)
const MAX_PROTOBUF_CHUNK_SIZE: usize = 3700;

impl AsyncMessageHandler {
    fn new() -> Self {
        // Initialize BOM weather provider (no API key needed)
        let mut weather_provider = UnifiedWeatherProvider::new(
            WeatherProviderType::Bom,
            Some(600), // Cache for 10 minutes
        );

        // Enable air quality if OpenAQ API key is provided
        if let Ok(api_key) = std::env::var("OPENAQ_API_KEY") {
            if !api_key.is_empty() {
                log::info!("🌍 Enabling OpenAQ air quality support");
                weather_provider.enable_air_quality(api_key, Some(600));
            }
        }

        let weather_provider = Arc::new(weather_provider);

        let handler = Self {
            communicator: Arc::new(Mutex::new(None)),
            initialization_complete: Arc::new(Mutex::new(false)),
            notification_handler: Arc::new(Mutex::new(None)),
            pairing_detected: Arc::new(Mutex::new(false)),
            message_queue: Arc::new(Mutex::new(VecDeque::new())),
            send_semaphore: Arc::new(Semaphore::new(1)),
            data_transfer_handler: Arc::new(DataTransferHandler::new()),
            watchdog: Arc::new(Mutex::new(None)),
            calendar_manager: Arc::new(Mutex::new(None)),
            pending_protobuf_chunks: Arc::new(Mutex::new(HashMap::new())),
            weather_provider,
        };

        // Start the message queue processor
        let queue = handler.message_queue.clone();
        let comm = handler.communicator.clone();
        let sem = handler.send_semaphore.clone();

        tokio::spawn(async move {
            loop {
                // Wait for a message in the queue
                let message = {
                    let mut q = queue.lock().unwrap();
                    q.pop_front()
                };

                if let Some(msg) = message {
                    // Acquire semaphore to ensure only one message is being sent at a time
                    let _permit = sem.acquire().await.unwrap();

                    // Get communicator and send
                    let comm_ref = {
                        let c = comm.lock().unwrap();
                        c.as_ref().cloned()
                    };

                    if let Some(communicator) = comm_ref {
                        match communicator.send_message("queued_message", &msg).await {
                            Ok(()) => {}
                            Err(e) => {
                                error!("   ❌ [QUEUE] Failed to send message: {}", e);
                            }
                        }
                    }

                    // Small delay between messages to allow for ACKs
                    tokio::time::sleep(Duration::from_millis(50)).await;
                } else {
                    // No message, sleep briefly and check again
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        });

        handler
    }

    fn set_communicator(&self, comm: Arc<CommunicatorV2>) {
        *self.communicator.lock().unwrap() = Some(comm);
    }

    fn set_notification_handler(&self, handler: Arc<GarminNotificationHandler>) {
        *self.notification_handler.lock().unwrap() = Some(handler.clone());
        // Also set watchdog reference
        *self.watchdog.lock().unwrap() = Some(handler.watchdog.clone());
    }

    fn set_calendar_manager(&self, manager: Arc<CalendarManager>) {
        *self.calendar_manager.lock().unwrap() = Some(manager);
    }

    /// Send a protobuf response, chunking it if necessary
    /// This handles large protobuf messages that exceed MAX_PROTOBUF_CHUNK_SIZE
    async fn send_protobuf_response(&self, message: Vec<u8>) -> types::Result<()> {
        // Parse the message to extract protobuf payload
        if message.len() < 18 {
            // Message too small to be a valid ProtobufResponse, send as-is
            return self.send_response(&message).await;
        }

        // Extract fields from ProtobufResponse message
        let message_id = u16::from_le_bytes([message[2], message[3]]);
        if message_id != 0x13B4 {
            // Not a ProtobufResponse, send as-is
            return self.send_response(&message).await;
        }

        let request_id = u16::from_le_bytes([message[4], message[5]]);

        // Extract protobuf payload (starts at byte 18, ends at -2 for checksum)
        let protobuf_payload = message[18..message.len() - 2].to_vec();

        // Check if we need to chunk
        if protobuf_payload.len() <= MAX_PROTOBUF_CHUNK_SIZE {
            // Small enough to send as-is
            info!("   ✅ Message fits in one chunk, sending directly");
            return self.send_response(&message).await;
        }

        // Need to chunk - store the complete payload and send first chunk
        info!(
            "   📦 Message too large, chunking into {}-byte chunks",
            MAX_PROTOBUF_CHUNK_SIZE
        );

        // Store the pending chunk
        {
            let mut pending = self.pending_protobuf_chunks.lock().unwrap();
            pending.insert(
                request_id,
                PendingProtobufChunk {
                    complete_payload: protobuf_payload.clone(),
                    total_length: protobuf_payload.len(),
                    message_type: message_id,
                    request_id,
                },
            );
            info!("   📦 Stored pending chunk for request ID {}", request_id);
        }

        // Build first chunk
        let first_chunk = &protobuf_payload[..MAX_PROTOBUF_CHUNK_SIZE];
        debug!("   📦 First chunk details:");
        debug!("      Start offset: 0");
        debug!("      End offset: {}", MAX_PROTOBUF_CHUNK_SIZE);
        debug!("      Chunk size: {} bytes", first_chunk.len());
        debug!("      First 32 bytes: {}", hex_dump(first_chunk, 32));

        self.send_protobuf_chunk(request_id, 0, first_chunk, protobuf_payload.len())
            .await
    }

    /// Send a specific chunk of a protobuf message
    async fn send_protobuf_chunk(
        &self,
        request_id: u16,
        data_offset: u32,
        chunk_data: &[u8],
        total_length: usize,
    ) -> types::Result<()> {
        debug!("   📤 Sending protobuf chunk:");
        debug!("      Request ID: {}", request_id);
        debug!("      Offset: {}", data_offset);
        debug!("      Chunk size: {} bytes", chunk_data.len());
        debug!("      Total size: {} bytes", total_length);
        debug!(
            "      Progress: {}/{} bytes ({:.1}%)",
            data_offset + chunk_data.len() as u32,
            total_length,
            ((data_offset + chunk_data.len() as u32) as f64 / total_length as f64) * 100.0
        );
        debug!(
            "      Chunk data (first 32 bytes): {}",
            hex_dump(chunk_data, 32)
        );

        // Build ProtobufResponse message with this chunk
        let mut message = Vec::new();

        // Packet size placeholder
        message.extend_from_slice(&[0u8, 0u8]);

        // Message ID: PROTOBUF_RESPONSE (5044 = 0x13B4)
        message.extend_from_slice(&5044u16.to_le_bytes());

        // Request ID
        message.extend_from_slice(&request_id.to_le_bytes());

        // Data offset
        message.extend_from_slice(&data_offset.to_le_bytes());

        // Total protobuf length
        message.extend_from_slice(&(total_length as u32).to_le_bytes());

        // Protobuf data length (size of this chunk)
        message.extend_from_slice(&(chunk_data.len() as u32).to_le_bytes());

        // Protobuf payload (this chunk)
        message.extend_from_slice(chunk_data);

        // Fill in packet size
        let packet_size = (message.len() + 2) as u16;
        message[0..2].copy_from_slice(&packet_size.to_le_bytes());

        // Add checksum (CRC-16)
        let checksum = compute_crc16(&message);
        message.extend_from_slice(&checksum.to_le_bytes());

        debug!("   📤 Chunk message size: {} bytes", message.len());

        // Send the chunk
        self.send_response(&message).await
    }

    /// Handle ACK for a protobuf chunk - send next chunk if needed
    async fn handle_protobuf_chunk_ack(
        &self,
        request_id: u16,
        data_offset: u32,
    ) -> types::Result<()> {
        let pending_chunk = {
            let pending = self.pending_protobuf_chunks.lock().unwrap();
            pending.get(&request_id).cloned()
        };

        if let Some(chunk_info) = pending_chunk {
            let next_offset = data_offset as usize + MAX_PROTOBUF_CHUNK_SIZE;

            debug!("   📦 Checking for next chunk:");
            debug!("      Request ID: {}", request_id);
            debug!("      Current offset (from ACK): {}", data_offset);
            debug!("      Chunk size sent: {} bytes", MAX_PROTOBUF_CHUNK_SIZE);
            debug!(
                "      Next offset calculation: {} + {} = {}",
                data_offset, MAX_PROTOBUF_CHUNK_SIZE, next_offset
            );
            debug!("      Total length: {}", chunk_info.total_length);
            debug!(
                "      Remaining: {} bytes",
                chunk_info.total_length.saturating_sub(next_offset)
            );

            if next_offset < chunk_info.total_length {
                // More chunks to send
                let remaining = chunk_info.total_length - next_offset;
                let chunk_size = remaining.min(MAX_PROTOBUF_CHUNK_SIZE);

                // Verify we're not reading beyond bounds
                if next_offset + chunk_size > chunk_info.complete_payload.len() {
                    error!(
                        "   ❌ ERROR: Chunk boundary exceeds payload! offset={}, size={}, total={}",
                        next_offset,
                        chunk_size,
                        chunk_info.complete_payload.len()
                    );
                    return Ok(());
                }

                let next_chunk =
                    &chunk_info.complete_payload[next_offset..next_offset + chunk_size];

                debug!(
                    "   📤 Sending next chunk (offset {}, {} bytes)",
                    next_offset, chunk_size
                );
                debug!(
                    "      Extracting bytes: {}..{}",
                    next_offset,
                    next_offset + chunk_size
                );
                debug!(
                    "      First 32 bytes of chunk: {}",
                    hex_dump(next_chunk, 32)
                );

                self.send_protobuf_chunk(
                    request_id,
                    next_offset as u32,
                    next_chunk,
                    chunk_info.total_length,
                )
                .await?;
            } else {
                // All chunks sent, remove from pending
                info!("   ✅ All chunks sent for request ID {}", request_id);
                info!("      Final offset: {}", next_offset);
                info!("      Total transferred: {} bytes", chunk_info.total_length);
                let mut pending = self.pending_protobuf_chunks.lock().unwrap();
                pending.remove(&request_id);
                debug!("   🎉 Transfer complete! Removed from pending chunks.");
            }
        } else {
            error!(
                "   ℹ️  No pending chunk found for request ID {}",
                request_id
            );
        }

        Ok(())
    }

    async fn send_response(&self, response: &[u8]) -> types::Result<()> {
        let mut queue = self.message_queue.lock().unwrap();
        queue.push_back(response.to_vec());

        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncGfdiMessageCallback for AsyncMessageHandler {
    async fn on_message(&self, message: &[u8]) -> types::Result<Option<Vec<u8>>> {
        // Record RX traffic for watchdog
        let watchdog = self.watchdog.lock().unwrap().as_ref().cloned();
        if let Some(wd) = watchdog {
            wd.record_rx().await;
        }

        if message.is_empty() {
            return Ok(None);
        }

        match MessageParser::parse(message) {
            Ok(parsed_message) => {
                match parsed_message {
                    crate::GfdiMessage::DeviceInformation(dev_info) => {
                        info!("📱 Received DeviceInformation:");
                        info!("   Protocol: {}", dev_info.protocol_version);
                        info!("   Product: {}", dev_info.product_number);
                        info!("   Unit: {}", dev_info.unit_number);
                        info!("   SW Version: {}", dev_info.software_version);
                        info!("   Max Packet: {}", dev_info.max_packet_size);
                        info!("   Name: {}", dev_info.bluetooth_friendly_name);
                        info!("   Model: {}", dev_info.device_model);

                        // Update communicator with device-reported max packet size
                        let comm_ref = {
                            let c = self.communicator.lock().unwrap();
                            c.as_ref().cloned()
                        };
                        if let Some(communicator) = comm_ref {
                            communicator
                                .on_device_max_packet_size(dev_info.max_packet_size)
                                .await;
                        }

                        // Generate and send response immediately
                        match MessageGenerator::device_information_response(&dev_info) {
                            Ok(response) => {
                                self.send_response(&response).await?;
                            }
                            Err(e) => {
                                error!("   ❌ Failed to generate response: {}", e);
                            }
                        }
                    }
                    crate::GfdiMessage::Configuration(config) => {
                        info!(
                            "📱 Received Configuration with {} capabilities",
                            config.capabilities.len()
                        );

                        // Mark that we've received Configuration (device is paired)
                        *self.pairing_detected.lock().unwrap() = true;
                        info!("   🔔 PAIRING DETECTED FLAG SET TO TRUE");

                        // Generate and send response immediately
                        match MessageGenerator::configuration_response() {
                            Ok(response) => {
                                self.send_response(&response).await?;
                            }
                            Err(e) => {
                                error!("   ❌ Failed to generate response: {}", e);
                            }
                        }

                        // FIRST: Send Configuration proactively to advertise HTTP capabilities
                        info!("   📤 Sending Configuration proactively to advertise HTTP capabilities");
                        match MessageGenerator::configuration_response() {
                            Ok(msg) => {
                                self.send_response(&msg).await?;
                                info!("      ✅ Configuration with HTTP capability #10 sent!");
                            }
                            Err(e) => error!("      ❌ Failed to send Configuration: {}", e),
                        }

                        // Send device settings to enable notifications on watch
                        info!("   📤 Sending SetDeviceSettings (enable notifications)");
                        match MessageGenerator::set_device_settings(true, true, false) {
                            Ok(msg) => {
                                self.send_response(&msg).await?;
                                info!("      ✅ SetDeviceSettings sent");
                            }
                            Err(e) => {
                                error!("      ❌ Failed to send SetDeviceSettings: {}", e)
                            }
                        }

                        // Send SYNC_READY to indicate we're ready to sync
                        info!("   📤 Sending SYNC_READY event");
                        match MessageGenerator::system_event(8, 0) {
                            Ok(msg) => {
                                self.send_response(&msg).await?;
                                info!("      ✅ SYNC_READY sent");
                            }
                            Err(e) => error!("      ❌ Failed to send SYNC_READY: {}", e),
                        }

                        // Send HOST_DID_ENTER_FOREGROUND to trigger notification subscription
                        info!("   📤 Sending HOST_DID_ENTER_FOREGROUND");
                        match MessageGenerator::system_event(6, 0) {
                            Ok(msg) => {
                                self.send_response(&msg).await?;
                                info!("      ✅ HOST_DID_ENTER_FOREGROUND sent");
                            }
                            Err(e) => {
                                error!("      ❌ Failed to send HOST_DID_ENTER_FOREGROUND: {}", e)
                            }
                        }

                        // Mark initialization as complete (even if we skipped sending messages)
                        let is_first_connect = {
                            let mut init_complete = self.initialization_complete.lock().unwrap();
                            if *init_complete {
                                false
                            } else {
                                *init_complete = true;
                                true
                            }
                        };

                        if is_first_connect {
                            info!("   🎉 First connect - sending SYNC_COMPLETE");
                            match MessageGenerator::system_event(0, 0) {
                                Ok(msg) => {
                                    info!("      ✅ Sending SYNC_COMPLETE event");
                                    self.send_response(&msg).await?;
                                }
                                Err(e) => error!("      ❌ Failed to send SYNC_COMPLETE: {}", e),
                            }
                        }

                        info!("   ✅ Minimal initialization complete - watch should now subscribe to notifications");
                    }
                    crate::GfdiMessage::CurrentTimeRequest => {
                        info!("📱 Received CurrentTimeRequest");

                        match MessageGenerator::current_time_response() {
                            Ok(response) => {
                                info!("   ✅ Generated CurrentTime response");
                                self.send_response(&response).await?;
                                info!("   ✅ CurrentTime response sent!");
                            }
                            Err(e) => {
                                error!("   ❌ Failed to generate response: {}", e);
                            }
                        }
                    }
                    crate::GfdiMessage::NotificationControl(ctrl) => {
                        debug!(
                            "📱 Received NotificationControl: ID={}, Command={}",
                            ctrl.notification_id, ctrl.command
                        );

                        // Log action details if present (for command 128)
                        if let Some(action_id) = ctrl.action_id {
                            let action_name = match action_id {
                                1..=5 => format!("CUSTOM_ACTION_{}", action_id),
                                94 => "REPLY_INCOMING_CALL".to_string(),
                                95 => "REPLY_MESSAGES".to_string(),
                                96 => "ACCEPT_INCOMING_CALL".to_string(),
                                97 => "REJECT_INCOMING_CALL".to_string(),
                                98 => "DISMISS_NOTIFICATION".to_string(),
                                99 => "BLOCK_APPLICATION".to_string(),
                                _ => format!("UNKNOWN({})", action_id),
                            };
                            debug!("   Action: {} (id={})", action_name, action_id);
                            if let Some(ref action_string) = ctrl.action_string {
                                debug!("   Action String: {:?}", action_string);
                            }
                        }

                        debug!(
                            "   Requested attributes: {} attributes",
                            ctrl.attributes.len()
                        );
                        for (attr_id, max_len) in &ctrl.attributes {
                            let attr_name = match attr_id {
                                0 => "APP_IDENTIFIER",
                                1 => "TITLE",
                                2 => "SUBTITLE",
                                3 => "MESSAGE",
                                4 => "MESSAGE_SIZE",
                                5 => "DATE",
                                127 => "ACTIONS",
                                _ => "UNKNOWN",
                            };
                            debug!(
                                "      - {} (id={}, max_len={})",
                                attr_name, attr_id, max_len
                            );
                        }

                        // Handle the notification control request
                        let handler_clone =
                            self.notification_handler.lock().unwrap().as_ref().cloned();
                        if let Some(handler) = handler_clone {
                            if let Err(e) = handler
                                .on_notification_control(
                                    ctrl.notification_id,
                                    ctrl.command,
                                    &ctrl.attributes,
                                )
                                .await
                            {
                                error!("   ❌ Failed to handle NotificationControl: {}", e);
                            }
                        } else {
                            error!("   ❌ Notification handler not available");
                        }
                    }
                    crate::GfdiMessage::NotificationSubscription(sub) => {
                        debug!("📱 Received NotificationSubscription:");
                        debug!("   Enable: {}", sub.enable);
                        debug!("   Unknown: {}", sub.unk);

                        // Generate and send response
                        match MessageGenerator::notification_subscription_response(&sub, true) {
                            Ok(response) => {
                                debug!("   ✅ Generated NotificationSubscription response");
                                self.send_response(&response).await?;
                                debug!("   ✅ NotificationSubscription response sent!");
                            }
                            Err(e) => {
                                eprintln!("   ❌ Failed to generate response: {}", e);
                            }
                        }
                    }
                    crate::GfdiMessage::Synchronization(sync_msg) => {
                        debug!("📱 Received Synchronization:");
                        debug!("   Type: {}", sync_msg.synchronization_type);
                        debug!("   Bitmask: 0x{:016X}", sync_msg.file_type_bitmask);
                        debug!("   Should proceed: {}", sync_msg.should_proceed());

                        // Send ACK for synchronization message
                        match MessageGenerator::synchronization_ack() {
                            Ok(response) => {
                                self.send_response(&response).await?;

                                // If should proceed, send filter message
                                if sync_msg.should_proceed() {
                                    match MessageGenerator::filter_message(3) {
                                        Ok(filter) => {
                                            self.send_response(&filter).await?;
                                        }
                                        Err(e) => {
                                            error!("   ❌ Failed to generate filter: {}", e);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!("   ❌ Failed to generate ACK: {}", e);
                            }
                        }
                    }
                    crate::GfdiMessage::FilterStatus(filter_status) => {
                        println!("📱 Received FilterStatus:");
                        println!("   Status: {:?}", filter_status.status);
                        println!("   Filter type: {}", filter_status.filter_type);
                    }
                    crate::GfdiMessage::WeatherRequest(weather_req) => {
                        let lat_deg = weather_req.latitude as f64 / 11930464.7111;
                        let lon_deg = weather_req.longitude as f64 / 11930464.7111;

                        println!("🌤️  Received WeatherRequest:");
                        println!("   Format: {}", weather_req.format);
                        println!(
                            "   Latitude: {:.4}° ({} semicircles)",
                            lat_deg, weather_req.latitude
                        );
                        println!(
                            "   Longitude: {:.4}° ({} semicircles)",
                            lon_deg, weather_req.longitude
                        );
                        println!("   Hours of forecast: {}", weather_req.hours_of_forecast);

                        println!("      Sending ACK only, no weather data available");

                        // Send ACK response to weather request
                        match MessageGenerator::weather_response(&weather_req) {
                            Ok(response) => {
                                println!("   ✅ Sent Weather ACK response");
                                self.send_response(&response).await?;
                            }
                            Err(e) => {
                                eprintln!("   ❌ Failed to generate weather ACK: {}", e);
                            }
                        }
                    }
                    crate::GfdiMessage::Unknown { message_id, data } => {
                        println!("📱 Received UNKNOWN message: ID=0x{:04X}", message_id);
                        println!("   Data: {}", hex_dump(&data, 32));

                        // Handle MusicControlCapabilities (0x13B2 = 5042)
                        if message_id == 0x13B2 {
                            println!("   ℹ️  MusicControlCapabilities request");
                            println!("   💡 Music control not fully implemented - sending ACK");

                            // Send simple ACK for MusicControlCapabilities
                            let response_payload = vec![
                                0xB2,
                                0x13, // Original message ID: MusicControlCapabilities (5042)
                                0x00, // Status: ACK
                            ];

                            let response = wrap_in_gfdi_envelope(0x1388, &response_payload);

                            let comm = self.communicator.lock().unwrap().as_ref().unwrap().clone();
                            match comm.send_message("music_control_ack", &response).await {
                                Ok(_) => println!("   ✅ MusicControlCapabilities ACK sent"),
                                Err(e) => eprintln!("   ❌ Failed to send ACK: {}", e),
                            }
                        }
                        // Handle Response/Status messages (0x1388 = 5000)
                        else if message_id == 0x1388 {
                            println!("   📨 RESPONSE/STATUS MESSAGE (0x1388)");
                            println!(
                                "      Full data ({} bytes): {}",
                                data.len(),
                                hex_dump(&data, data.len())
                            );

                            // Parse Response message
                            if data.len() >= 4 {
                                let original_msg_id = u16::from_le_bytes([data[0], data[1]]);
                                let status = data[2];

                                println!("      Original message ID: 0x{:04X}", original_msg_id);
                                println!("      Status byte: 0x{:02X}", status);

                                // Check if this is an ACK for a ProtobufResponse (0x13B4) - need to send next chunk
                                if original_msg_id == 0x13B4 && status == 0 && data.len() >= 11 {
                                    let request_id = u16::from_le_bytes([data[3], data[4]]);
                                    let data_offset =
                                        u32::from_le_bytes([data[5], data[6], data[7], data[8]]);

                                    println!("   📨 ACK for ProtobufResponse chunk");
                                    println!("      Original msg ID: 0x{:04X}", original_msg_id);
                                    println!(
                                        "      Status: {} ({})",
                                        status,
                                        if status == 0 { "ACK" } else { "NACK" }
                                    );
                                    println!("      Request ID: {}", request_id);
                                    println!("      Data offset (from ACK): {}", data_offset);
                                    println!("      ACK data length: {} bytes", data.len());
                                    println!(
                                        "      ACK data (hex): {}",
                                        hex_dump(&data, data.len())
                                    );

                                    // Handle the chunk ACK (send next chunk if needed)
                                    if let Err(e) = self
                                        .handle_protobuf_chunk_ack(request_id, data_offset)
                                        .await
                                    {
                                        eprintln!(
                                            "   ❌ Failed to handle protobuf chunk ACK: {}",
                                            e
                                        );
                                    }
                                }

                                // Check if this is a response to NotificationData (0x13AB = 5035)
                                if original_msg_id == 0x13AB {
                                    let transfer_status = if data.len() > 3 { data[3] } else { 0 };

                                    println!("   🔔 Response to NotificationData (0x13AB)");
                                    println!(
                                        "      Status: {} ({})",
                                        status,
                                        if status == 0 { "ACK" } else { "ERROR" }
                                    );
                                    println!(
                                        "      Transfer Status: 0x{:02X} ({})",
                                        transfer_status,
                                        match transfer_status {
                                            0 => "OK",
                                            1 => "RESEND",
                                            2 => "ABORT",
                                            3 => "CRC_MISMATCH",
                                            4 => "OFFSET_MISMATCH",
                                            _ => "UNKNOWN",
                                        }
                                    );

                                    // If status is ACK (0) and transfer is OK (0), the upload is complete
                                    if status == 0 && transfer_status == 0 {
                                        println!(
                                            "   ✅ NotificationData upload confirmed by watch!"
                                        );
                                        println!(
                                            "      💡 Watch has accepted the notification data"
                                        );
                                        println!("      💡 Now sending final status ACK to complete handshake...");

                                        // Send final status ACK back to watch to complete the upload handshake
                                        // This is the missing piece that causes the MESSAGE_SIZE loop!
                                        let mut final_status_payload = Vec::new();
                                        final_status_payload
                                            .extend_from_slice(&0x13ABu16.to_le_bytes()); // NotificationData
                                        final_status_payload.push(0x00); // Status: ACK
                                        final_status_payload.push(0x00); // TransferStatus: OK

                                        let final_status_msg =
                                            wrap_in_gfdi_envelope(0x1388, &final_status_payload);

                                        println!(
                                            "      📤 Sending final status ACK (0x1388) to watch"
                                        );
                                        println!(
                                            "         Payload: {:02X} {:02X} {:02X} {:02X}",
                                            final_status_payload[0],
                                            final_status_payload[1],
                                            final_status_payload[2],
                                            final_status_payload[3]
                                        );

                                        // Send the final status using the communicator
                                        let comm = self
                                            .communicator
                                            .lock()
                                            .unwrap()
                                            .as_ref()
                                            .unwrap()
                                            .clone();
                                        match comm
                                            .send_message("upload_complete", &final_status_msg)
                                            .await
                                        {
                                            Ok(_) => {
                                                println!(
                                                    "      ✅ Final status ACK sent successfully!"
                                                );
                                                println!("      🎉 Upload handshake complete - notification should appear on watch!");
                                            }
                                            Err(e) => {
                                                eprintln!(
                                                    "      ❌ Failed to send final status ACK: {}",
                                                    e
                                                );
                                            }
                                        }
                                    } else if transfer_status == 3 {
                                        println!("   ❌ CRC mismatch - watch rejected the data!");
                                        println!("      💡 The watch calculated a different CRC");
                                        println!("      💡 This indicates data corruption or incorrect CRC calculation");
                                    } else if transfer_status == 4 {
                                        println!("   ❌ Offset mismatch - chunking issue!");
                                        println!("      💡 The watch expected data at a different offset");
                                        println!(
                                            "      💡 This indicates chunking protocol mismatch"
                                        );
                                    } else if status != 0 {
                                        println!("   ⚠️  Watch returned error status: {}", status);
                                    } else if transfer_status == 1 {
                                        println!("   ⚠️  Watch requesting RESEND");
                                        println!("      💡 We may need to resend the same data");
                                    } else if transfer_status == 2 {
                                        println!("   ⚠️  Watch sent ABORT");
                                        println!("      💡 Upload was cancelled by watch");
                                    }
                                } else {
                                    let msg_name = match original_msg_id {
                                        0x13A9 => "NotificationUpdate",
                                        0x13AA => "NotificationControl",
                                        0x13AC => "NotificationSubscription",
                                        0x13A0 => "DeviceInformation",
                                        0x13BA => "Configuration",
                                        _ => "Unknown",
                                    };
                                    println!(
                                        "   Response to message: 0x{:04X} ({}), status: {}",
                                        original_msg_id, msg_name, status
                                    );
                                }
                            } else {
                                println!("   ⚠️  Response data too short ({} bytes)", data.len());
                            }
                        }
                        // Handle ProtobufRequest (0x13B3 = 5043)
                        else if message_id == 0x13B3 {
                            println!("   ℹ️  This is a ProtobufRequest - parsing protobuf payload");

                            // Parse the full ProtobufRequest header:
                            // - requestId (2 bytes)
                            // - dataOffset (4 bytes)
                            // - totalProtobufLength (4 bytes)
                            // - protobufDataLength (4 bytes)
                            // - messageBytes (protobufDataLength bytes)
                            if data.len() >= 18 {
                                let request_id = u16::from_le_bytes([data[0], data[1]]);
                                let data_offset =
                                    u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
                                let total_protobuf_length =
                                    u32::from_le_bytes([data[6], data[7], data[8], data[9]]);
                                let protobuf_data_length =
                                    u32::from_le_bytes([data[10], data[11], data[12], data[13]]);

                                println!("   Request ID: {}", request_id);
                                println!("   Data Offset: {}", data_offset);
                                println!("   Total Protobuf Length: {}", total_protobuf_length);
                                println!("   Protobuf Data Length: {}", protobuf_data_length);

                                // Extract protobuf payload
                                let protobuf_start = 14;
                                let protobuf_end = protobuf_start + protobuf_data_length as usize;

                                if data.len() >= protobuf_end {
                                    let protobuf_payload = &data[protobuf_start..protobuf_end];
                                    println!(
                                        "   Protobuf payload hex: {}",
                                        hex_dump(protobuf_payload, protobuf_payload.len())
                                    );

                                    // Check if this is a complete message (not chunked)
                                    let is_complete = data_offset == 0
                                        && total_protobuf_length == protobuf_data_length;

                                    if is_complete {
                                        println!("   ℹ️  Complete protobuf message - attempting to parse");

                                        // Try to identify the protobuf content
                                        // Common patterns:
                                        // - Field 8 (device_status_service): tag byte = (8 << 3) | 2 = 0x42
                                        // - Field 1 (core_service): tag byte = (1 << 3) | 2 = 0x0A
                                        // - Field 2 (calendar_service): tag byte = (2 << 3) | 2 = 0x12

                                        if protobuf_payload.len() > 0 {
                                            let first_tag = protobuf_payload[0];
                                            let field_number = first_tag >> 3;
                                            let wire_type = first_tag & 0x07;

                                            println!(
                                                "   First protobuf field: {} (wire type: {})",
                                                field_number, wire_type
                                            );

                                            // Field 1 is calendar_service or core_service
                                            if field_number == 1 && wire_type == 2 {
                                                println!("   ℹ️  Detected CalendarService/CoreService request (field 1)");

                                                // Try to parse as calendar request
                                                match parse_calendar_request(protobuf_payload) {
                                                    Ok(calendar_request) => {
                                                        println!("   📅 Calendar Service Request detected!");
                                                        println!(
                                                            "      Time range: {} to {}",
                                                            calendar_request.start_date,
                                                            calendar_request.end_date
                                                        );
                                                        println!(
                                                            "      Max events: {}",
                                                            calendar_request.max_events
                                                        );
                                                        println!(
                                                            "      Include all-day: {}",
                                                            calendar_request.include_all_day
                                                        );

                                                        // Send ACK first before processing calendar request
                                                        println!("      📤 Sending ACK for calendar request...");
                                                        let mut ack_payload = vec![
                                                            0xB3,
                                                            0x13, // Original message ID: ProtobufRequest (5043)
                                                            0x00, // Status: ACK
                                                        ];
                                                        ack_payload.extend_from_slice(
                                                            &request_id.to_le_bytes(),
                                                        );
                                                        ack_payload.extend_from_slice(
                                                            &data_offset.to_le_bytes(),
                                                        );
                                                        ack_payload.push(0x00); // ProtobufChunkStatus: KEPT
                                                        ack_payload.push(0x00); // ProtobufStatusCode: NO_ERROR

                                                        let ack_response = wrap_in_gfdi_envelope(
                                                            5000,
                                                            &ack_payload,
                                                        );

                                                        if let Err(e) =
                                                            self.send_response(&ack_response).await
                                                        {
                                                            eprintln!(
                                                                "      ❌ Failed to send ACK: {}",
                                                                e
                                                            );
                                                        } else {
                                                            println!("      ✅ Calendar request ACK sent");
                                                        }

                                                        // Get calendar manager
                                                        let calendar_manager = self
                                                            .calendar_manager
                                                            .lock()
                                                            .unwrap()
                                                            .clone();

                                                        if calendar_manager.is_none() {
                                                            println!("      ⚠️  Calendar manager not initialized - sending empty response");
                                                        }

                                                        // Handle the calendar request asynchronously with timeout
                                                        let calendar_mgr_opt = calendar_manager
                                                            .as_ref()
                                                            .map(|m| m.as_ref());

                                                        println!(
                                                            "      🔄 Fetching calendar events..."
                                                        );
                                                        let fetch_result = tokio::time::timeout(
                                                            Duration::from_secs(15),
                                                            handle_calendar_request(
                                                                &calendar_request,
                                                                calendar_mgr_opt,
                                                            ),
                                                        )
                                                        .await;

                                                        match fetch_result {
                                                            Ok(Ok(events)) => {
                                                                println!("      ✅ Fetched {} calendar events", events.len());

                                                                // Encode the calendar response
                                                                let response_data = encode_calendar_response(
                                                                    &events,
                                                                    CalendarResponseStatus::Ok,
                                                                    request_id,
                                                                    calendar_request.use_core_service_envelope,
                                                                );

                                                                // Wrap in GFDI envelope with message ID 0x13B4 (ProtobufResponse = 5044)
                                                                let response =
                                                                    wrap_in_gfdi_envelope(
                                                                        0x13B4,
                                                                        &response_data,
                                                                    );

                                                                println!(
                                                                    "      📤 Sending calendar response ({} bytes)",
                                                                    response_data.len()
                                                                );

                                                                // Send the response
                                                                match {
                                                                    let comm = self
                                                                        .communicator
                                                                        .lock()
                                                                        .unwrap()
                                                                        .as_ref()
                                                                        .unwrap()
                                                                        .clone();
                                                                    comm.send_message("calendar_response", &response)
                                                                        .await
                                                                } {
                                                                    Ok(_) => println!(
                                                                        "      ✅ Calendar response sent successfully!"
                                                                    ),
                                                                    Err(e) => eprintln!(
                                                                        "      ❌ Failed to send calendar response: {}",
                                                                        e
                                                                    ),
                                                                }
                                                            }
                                                            Err(_timeout) => {
                                                                eprintln!(
                                                                    "      ❌ Calendar request timeout"
                                                                );

                                                                // Send error response
                                                                let response_data = encode_calendar_response(
                                                                    &[],
                                                                    CalendarResponseStatus::InvalidDateRange,
                                                                    request_id,
                                                                    calendar_request.use_core_service_envelope,
                                                                );

                                                                let response =
                                                                    wrap_in_gfdi_envelope(
                                                                        0x13B4,
                                                                        &response_data,
                                                                    );

                                                                match {
                                                                    let comm = self
                                                                        .communicator
                                                                        .lock()
                                                                        .unwrap()
                                                                        .as_ref()
                                                                        .unwrap()
                                                                        .clone();
                                                                    comm.send_message(
                                                                        "calendar_error_response",
                                                                        &response,
                                                                    )
                                                                    .await
                                                                } {
                                                                    Ok(_) => println!(
                                                                        "      ✅ Calendar error response sent"
                                                                    ),
                                                                    Err(e) => eprintln!(
                                                                        "      ❌ Failed to send error response: {}",
                                                                        e
                                                                    ),
                                                                }
                                                            }
                                                            Ok(Err(e)) => {
                                                                eprintln!(
                                                                    "      ❌ Calendar fetch error: {}",
                                                                    e
                                                                );
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        println!("      ⚠️  Not a calendar request or parse error: {}", e);
                                                        println!("      💡 Sending generic ACK");

                                                        // Send ACK for non-calendar CoreService requests
                                                        let mut response_payload = vec![
                                                            0xB3,
                                                            0x13, // Original message ID: ProtobufRequest (5043)
                                                            0x00, // Status: ACK
                                                        ];
                                                        response_payload.extend_from_slice(
                                                            &request_id.to_le_bytes(),
                                                        );
                                                        response_payload.extend_from_slice(
                                                            &data_offset.to_le_bytes(),
                                                        );
                                                        response_payload.push(0x00); // ProtobufChunkStatus: KEPT
                                                        response_payload.push(0x00); // ProtobufStatusCode: NO_ERROR

                                                        let response = wrap_in_gfdi_envelope(
                                                            5000,
                                                            &response_payload,
                                                        );

                                                        // Use queue to ensure proper message ordering
                                                        if let Err(e) =
                                                            self.send_response(&response).await
                                                        {
                                                            eprintln!(
                                                                "   ❌ Failed to queue ACK: {}",
                                                                e
                                                            );
                                                        } else {
                                                            println!("   ✅ Generic ACK queued");
                                                        }
                                                    }
                                                }
                                            }
                                            // Field 2 is http_service (HTTP requests over Bluetooth)
                                            else if field_number == 2 && wire_type == 2 {
                                                println!(
                                                    "   ℹ️  Detected HttpService request (field 2)"
                                                );

                                                // Parse the HTTP request
                                                match HttpRequest::parse(protobuf_payload) {
                                                    Ok(http_request) => {
                                                        let _request_field =
                                                            http_request.request_field;
                                                        println!(
                                                            "   🌐 HTTP {} {}",
                                                            http_request.method.as_str(),
                                                            http_request.path
                                                        );

                                                        // Clone weather provider for the blocking task
                                                        let weather_provider =
                                                            self.weather_provider.clone();

                                                        // Handle the request in a blocking task (HTTP I/O)
                                                        let http_response =
                                                            handle_http_request_with_weather(
                                                                &http_request,
                                                                Some(&weather_provider),
                                                            )
                                                            .await;
                                                        println!(
                                                            "   ✅ HTTP Response: {}",
                                                            http_response.status
                                                        );

                                                        // Encode as ProtobufResponse
                                                        match http_response
                                                            .encode_protobuf_response(
                                                                request_id,
                                                                &http_request,
                                                                Some(&self.data_transfer_handler),
                                                            ) {
                                                            Ok(response) => {
                                                                println!("   ✅ Sending ProtobufResponse with HTTP data");
                                                                match self.send_protobuf_response(response).await {
                                                                            Ok(_) => {
                                                                                println!("   ✅ HTTP response sent successfully!")
                                                                            }
                                                                            Err(e) => eprintln!("   ❌ Failed to send HTTP response: {}", e),
                                                                        }
                                                            }
                                                            Err(e) => {
                                                                eprintln!("   ❌ Failed to encode HTTP response: {}", e);
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        eprintln!("   ❌ Failed to parse HTTP request: {}", e);

                                                        // Send error ACK
                                                        let mut response_payload = vec![
                                                            0xB3,
                                                            0x13, // Original message ID: ProtobufRequest (5043)
                                                            0x01, // Status: NACK
                                                        ];
                                                        response_payload.extend_from_slice(
                                                            &request_id.to_le_bytes(),
                                                        );
                                                        response_payload.extend_from_slice(
                                                            &data_offset.to_le_bytes(),
                                                        );
                                                        response_payload.push(0x00); // ProtobufChunkStatus: KEPT
                                                        response_payload.push(0x00); // ProtobufStatusCode: NO_ERROR

                                                        let response = wrap_in_gfdi_envelope(
                                                            5000,
                                                            &response_payload,
                                                        );

                                                        // Use queue to ensure proper message ordering
                                                        if let Err(e) =
                                                            self.send_response(&response).await
                                                        {
                                                            eprintln!("   ❌ Failed to queue error ACK: {}", e);
                                                        } else {
                                                            println!("   ✅ Error ACK queued");
                                                        }
                                                    }
                                                }
                                            }
                                            // Field 8 is device_status_service (most common during initialization)
                                            else if field_number == 8 && wire_type == 2 {
                                                println!(
                                                    "   ℹ️  Detected DeviceStatusService request"
                                                );

                                                // For now, send a simple ACK response
                                                // In the future, we can parse the specific request and respond accordingly

                                                // Build ProtobufStatusMessage
                                                let mut response_payload = vec![
                                                    0xB3,
                                                    0x13, // Original message ID: ProtobufRequest (5043)
                                                    0x00, // Status: ACK
                                                ];
                                                response_payload
                                                    .extend_from_slice(&request_id.to_le_bytes());
                                                response_payload
                                                    .extend_from_slice(&data_offset.to_le_bytes());
                                                response_payload.push(0x00); // ProtobufChunkStatus: KEPT
                                                response_payload.push(0x00); // ProtobufStatusCode: NO_ERROR

                                                let response =
                                                    wrap_in_gfdi_envelope(5000, &response_payload);

                                                println!("   ✅ Sending ProtobufStatus ACK for DeviceStatusService");
                                                // Use queue to ensure proper message ordering
                                                if let Err(e) = self.send_response(&response).await
                                                {
                                                    eprintln!("   ❌ Failed to queue ACK: {}", e);
                                                } else {
                                                    println!("   ✅ ProtobufStatus ACK queued - watch should continue!");
                                                }
                                            }
                                            // Field 21 is LiveTrackService (live location tracking)
                                            else if field_number == 21 && wire_type == 2 {
                                                println!(
                                                    "   ℹ️  Detected LiveTrackService request"
                                                );
                                                println!("   💡 Live tracking not implemented - sending ACK");

                                                // Send ACK for LiveTrackService
                                                let mut response_payload = vec![
                                                    0xB3,
                                                    0x13, // Original message ID: ProtobufRequest (5043)
                                                    0x00, // Status: ACK
                                                ];
                                                response_payload
                                                    .extend_from_slice(&request_id.to_le_bytes());
                                                response_payload
                                                    .extend_from_slice(&data_offset.to_le_bytes());
                                                response_payload.push(0x00); // ProtobufChunkStatus: KEPT
                                                response_payload.push(0x00); // ProtobufStatusCode: NO_ERROR

                                                let response =
                                                    wrap_in_gfdi_envelope(5000, &response_payload);

                                                // Use queue to ensure proper message ordering
                                                if let Err(e) = self.send_response(&response).await
                                                {
                                                    eprintln!("   ❌ Failed to queue ACK: {}", e);
                                                } else {
                                                    println!("   ✅ LiveTrackService ACK queued");
                                                }
                                            }
                                            // Field 7 is data_transfer_service (DataTransfer chunk fetch requests)
                                            else if field_number == 7 && wire_type == 2 {
                                                println!("   ℹ️  Detected DataTransfer chunk fetch request (field 7)");

                                                // Parse DataTransfer request
                                                // Field 7 contains DataTransferItem with: id, offset, length
                                                let mut pos = 1; // Skip the field tag

                                                // Read length of field 7
                                                let mut length = 0usize;
                                                let mut shift = 0;
                                                while pos < protobuf_payload.len() {
                                                    let byte = protobuf_payload[pos];
                                                    pos += 1;
                                                    length |= ((byte & 0x7F) as usize) << shift;
                                                    if byte & 0x80 == 0 {
                                                        break;
                                                    }
                                                    shift += 7;
                                                }

                                                let field7_data =
                                                    &protobuf_payload[pos..pos + length];

                                                // Parse DataTransferItem: field 1 = id, field 2 = offset, field 3 = length
                                                let mut transfer_id: Option<u32> = None;
                                                let mut offset: Option<u32> = None;
                                                let mut chunk_length: Option<u32> = None;

                                                let mut parse_pos = 0;
                                                while parse_pos < field7_data.len() {
                                                    let tag = field7_data[parse_pos];
                                                    let field_num = tag >> 3;
                                                    parse_pos += 1;

                                                    let mut value = 0u32;
                                                    let mut shift = 0;
                                                    while parse_pos < field7_data.len() {
                                                        let byte = field7_data[parse_pos];
                                                        parse_pos += 1;
                                                        value |= ((byte & 0x7F) as u32) << shift;
                                                        if byte & 0x80 == 0 {
                                                            break;
                                                        }
                                                        shift += 7;
                                                    }

                                                    match field_num {
                                                        1 => transfer_id = Some(value),
                                                        2 => offset = Some(value),
                                                        3 => chunk_length = Some(value),
                                                        _ => {}
                                                    }
                                                }

                                                if let (Some(id), Some(off), Some(len)) =
                                                    (transfer_id, offset, chunk_length)
                                                {
                                                    println!("   📦 DataTransfer chunk request:");
                                                    println!("      Transfer ID: {}", id);
                                                    println!("      Offset: {}", off);
                                                    println!("      Length: {}", len);

                                                    // Get chunk from handler
                                                    if let Some(chunk) = self
                                                        .data_transfer_handler
                                                        .get_chunk(id, off as usize, len as usize)
                                                    {
                                                        println!(
                                                            "   ✅ Serving chunk: {} bytes",
                                                            chunk.len()
                                                        );

                                                        // Build DataDownloadResponse message
                                                        let mut download_response = Vec::new();

                                                        // Field 1: status (varint) = 1 (SUCCESS)
                                                        download_response.push((1 << 3) | 0);
                                                        download_response.push(1); // SUCCESS

                                                        // Field 2: id (varint)
                                                        download_response.push((2 << 3) | 0);
                                                        download_response.push(id as u8);

                                                        // Field 3: offset (varint)
                                                        download_response.push((3 << 3) | 0);
                                                        let mut off_val = off;
                                                        while off_val >= 0x80 {
                                                            download_response
                                                                .push((off_val as u8) | 0x80);
                                                            off_val >>= 7;
                                                        }
                                                        download_response.push(off_val as u8);

                                                        // Field 4: payload (bytes)
                                                        download_response.push((4 << 3) | 2);
                                                        let mut chunk_len = chunk.len();
                                                        while chunk_len >= 0x80 {
                                                            download_response
                                                                .push((chunk_len as u8) | 0x80);
                                                            chunk_len >>= 7;
                                                        }
                                                        download_response.push(chunk_len as u8);
                                                        download_response.extend_from_slice(&chunk);

                                                        // Wrap in DataTransferService field 2 (dataDownloadResponse)
                                                        let mut data_transfer_service = Vec::new();
                                                        data_transfer_service.push((2 << 3) | 2); // Field 2
                                                        let mut response_len =
                                                            download_response.len();
                                                        while response_len >= 0x80 {
                                                            data_transfer_service
                                                                .push((response_len as u8) | 0x80);
                                                            response_len >>= 7;
                                                        }
                                                        data_transfer_service
                                                            .push(response_len as u8);
                                                        data_transfer_service
                                                            .extend_from_slice(&download_response);

                                                        // Wrap in Smart proto field 7 (data_transfer_service)
                                                        let mut smart_proto = Vec::new();
                                                        smart_proto.push((7 << 3) | 2); // Field 7
                                                        let mut service_len =
                                                            data_transfer_service.len();
                                                        while service_len >= 0x80 {
                                                            smart_proto
                                                                .push((service_len as u8) | 0x80);
                                                            service_len >>= 7;
                                                        }
                                                        smart_proto.push(service_len as u8);
                                                        smart_proto.extend_from_slice(
                                                            &data_transfer_service,
                                                        );

                                                        // Build ProtobufResponse
                                                        let mut message = Vec::new();
                                                        message.extend_from_slice(&[0u8, 0u8]); // Size placeholder
                                                        message.extend_from_slice(
                                                            &5044u16.to_le_bytes(),
                                                        ); // PROTOBUF_RESPONSE
                                                        message.extend_from_slice(
                                                            &request_id.to_le_bytes(),
                                                        );
                                                        message
                                                            .extend_from_slice(&0u32.to_le_bytes()); // Data offset
                                                        message.extend_from_slice(
                                                            &(smart_proto.len() as u32)
                                                                .to_le_bytes(),
                                                        ); // Total length
                                                        message.extend_from_slice(
                                                            &(smart_proto.len() as u32)
                                                                .to_le_bytes(),
                                                        ); // Data length
                                                        message.extend_from_slice(&smart_proto);

                                                        // Fill in packet size
                                                        let packet_size =
                                                            (message.len() + 2) as u16;
                                                        message[0..2].copy_from_slice(
                                                            &packet_size.to_le_bytes(),
                                                        );

                                                        // Add checksum (CRC-16)
                                                        let checksum = compute_crc16(&message);
                                                        message.extend_from_slice(
                                                            &checksum.to_le_bytes(),
                                                        );

                                                        println!("   📤 Sending DataTransfer chunk response ({} bytes)", message.len());

                                                        let comm = self
                                                            .communicator
                                                            .lock()
                                                            .unwrap()
                                                            .as_ref()
                                                            .unwrap()
                                                            .clone();
                                                        match comm
                                                            .send_message(
                                                                "data_transfer_chunk",
                                                                &message,
                                                            )
                                                            .await
                                                        {
                                                            Ok(_) => println!(
                                                                "   ✅ DataTransfer chunk sent"
                                                            ),
                                                            Err(e) => eprintln!(
                                                                "   ❌ Failed to send chunk: {}",
                                                                e
                                                            ),
                                                        }
                                                    } else {
                                                        println!("   ❌ Transfer ID {} not found or invalid offset", id);

                                                        // Send error response (ACK with error status)
                                                        let mut response_payload =
                                                            vec![0xB3, 0x13, 0x01]; // NACK
                                                        response_payload.extend_from_slice(
                                                            &request_id.to_le_bytes(),
                                                        );
                                                        response_payload.extend_from_slice(
                                                            &data_offset.to_le_bytes(),
                                                        );
                                                        response_payload.push(0x00);
                                                        response_payload.push(0x01); // Error code

                                                        let response = wrap_in_gfdi_envelope(
                                                            5000,
                                                            &response_payload,
                                                        );

                                                        let comm = self
                                                            .communicator
                                                            .lock()
                                                            .unwrap()
                                                            .as_ref()
                                                            .unwrap()
                                                            .clone();
                                                        match comm
                                                            .send_message("data_transfer_error", &response)
                                                            .await
                                                        {
                                                            Ok(_) => println!(
                                                                "   ✅ DataTransfer error response sent"
                                                            ),
                                                            Err(e) => eprintln!(
                                                                "   ❌ Failed to send error: {}",
                                                                e
                                                            ),
                                                        }
                                                    }
                                                } else {
                                                    println!("   ❌ Failed to parse DataTransfer request fields");
                                                    println!("   💡 Not a DataTransfer request - sending generic ACK");

                                                    let mut response_payload =
                                                        vec![0xB3, 0x13, 0x00]; // ACK
                                                    response_payload.extend_from_slice(
                                                        &request_id.to_le_bytes(),
                                                    );
                                                    response_payload.extend_from_slice(
                                                        &data_offset.to_le_bytes(),
                                                    );
                                                    response_payload.push(0x00);
                                                    response_payload.push(0x00);

                                                    let response = wrap_in_gfdi_envelope(
                                                        5000,
                                                        &response_payload,
                                                    );

                                                    // Use queue to ensure proper message ordering
                                                    if let Err(e) =
                                                        self.send_response(&response).await
                                                    {
                                                        eprintln!(
                                                            "   ❌ Failed to queue ACK: {}",
                                                            e
                                                        );
                                                    } else {
                                                        println!("   ✅ Generic ACK queued");
                                                    }
                                                }
                                            } else {
                                                println!("   ⚠️  Unknown protobuf field {} - attempting HTTP parse", field_number);

                                                // Try parsing as HTTP request (watch might use different field numbers)
                                                match HttpRequest::parse(protobuf_payload) {
                                                    Ok(http_request) => {
                                                        let _request_field =
                                                            http_request.request_field;
                                                        println!(
                                                            "   🌐 HTTP {} {} (from field {})",
                                                            http_request.method.as_str(),
                                                            http_request.path,
                                                            field_number
                                                        );

                                                        // Clone weather provider for the blocking task
                                                        let weather_provider =
                                                            self.weather_provider.clone();

                                                        // Handle the request in a blocking task (HTTP I/O)
                                                        let http_response =
                                                            handle_http_request_with_weather(
                                                                &http_request,
                                                                Some(&weather_provider),
                                                            )
                                                            .await;
                                                        println!(
                                                            "   ✅ HTTP Response: {}",
                                                            http_response.status
                                                        );

                                                        // Encode as ProtobufResponse
                                                        match http_response
                                                            .encode_protobuf_response(
                                                                request_id,
                                                                &http_request,
                                                                Some(&self.data_transfer_handler),
                                                            ) {
                                                            Ok(response) => {
                                                                println!("   ✅ Sending ProtobufResponse with HTTP data");
                                                                // Use chunking-aware send for protobuf responses
                                                                if let Err(e) = self
                                                                    .send_protobuf_response(
                                                                        response,
                                                                    )
                                                                    .await
                                                                {
                                                                    eprintln!("   ❌ Failed to send HTTP response: {}", e);
                                                                } else {
                                                                    println!("   ✅ HTTP response queued");
                                                                }
                                                            }
                                                            Err(e) => {
                                                                eprintln!("   ❌ Failed to encode HTTP response: {}", e);
                                                            }
                                                        }
                                                    }
                                                    Err(_) => {
                                                        // Not an HTTP request, send generic ACK
                                                        println!("   💡 Not an HTTP request - sending generic ACK");

                                                        let mut response_payload = vec![
                                                            0xB3,
                                                            0x13, // Original message ID: ProtobufRequest (5043)
                                                            0x00, // Status: ACK
                                                        ];
                                                        response_payload.extend_from_slice(
                                                            &request_id.to_le_bytes(),
                                                        );
                                                        response_payload.extend_from_slice(
                                                            &data_offset.to_le_bytes(),
                                                        );
                                                        response_payload.push(0x00); // ProtobufChunkStatus: KEPT
                                                        response_payload.push(0x00); // ProtobufStatusCode: NO_ERROR

                                                        let response = wrap_in_gfdi_envelope(
                                                            5000,
                                                            &response_payload,
                                                        );

                                                        // Use queue to ensure proper message ordering
                                                        if let Err(e) =
                                                            self.send_response(&response).await
                                                        {
                                                            eprintln!(
                                                                "   ❌ Failed to queue ACK: {}",
                                                                e
                                                            );
                                                        } else {
                                                            println!("   ✅ Generic ACK queued");
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        println!("   ⚠️  Chunked protobuf message detected (not fully implemented)");
                                        println!(
                                            "   Data offset: {}, Total: {}, Current: {}",
                                            data_offset,
                                            total_protobuf_length,
                                            protobuf_data_length
                                        );

                                        // For chunked messages, send a KEPT status to acknowledge receipt
                                        let mut response_payload = vec![
                                            0xB3,
                                            0x13, // Original message ID: ProtobufRequest (5043)
                                            0x00, // Status: ACK
                                        ];
                                        response_payload
                                            .extend_from_slice(&request_id.to_le_bytes());
                                        response_payload
                                            .extend_from_slice(&data_offset.to_le_bytes());
                                        response_payload.push(0x00); // ProtobufChunkStatus: KEPT
                                        response_payload.push(0x00); // ProtobufStatusCode: NO_ERROR

                                        let response =
                                            wrap_in_gfdi_envelope(5000, &response_payload);

                                        // Use queue to ensure proper message ordering
                                        if let Err(e) = self.send_response(&response).await {
                                            eprintln!("   ❌ Failed to queue chunk ACK: {}", e);
                                        } else {
                                            println!("   ✅ Chunk ACK queued");
                                        }
                                    }
                                } else {
                                    eprintln!("   ❌ ProtobufRequest data too short for payload (expected {} bytes, got {})",
                                             protobuf_end, data.len());
                                }
                            } else {
                                eprintln!("   ❌ ProtobufRequest data too short to parse full header (got {} bytes, need 18)", data.len());
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("❌ Failed to parse GFDI message: {}", e);
                eprintln!("   Raw message: {}", hex_dump(message, 32));
            }
        }

        Ok(None)
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Wrap a message in GFDI envelope
fn wrap_in_gfdi_envelope(message_id: u16, payload: &[u8]) -> Vec<u8> {
    let mut message = Vec::new();

    // Calculate packet size - TOTAL packet size including size field itself
    // Java: payloadSize must equal byteBuffer.capacity() (total packet size)
    // Total = size field (2) + message_id (2) + payload (N) + CRC (2)
    let packet_size = (2 + 2 + payload.len() + 2) as u16;
    message.extend_from_slice(&packet_size.to_le_bytes());

    // Message ID (little-endian)
    message.extend_from_slice(&message_id.to_le_bytes());

    // Payload
    message.extend_from_slice(payload);

    // Calculate CRC over size + message_id + payload (everything so far)
    let crc = compute_crc(&message);
    message.extend_from_slice(&crc.to_le_bytes());

    message
}

fn compute_crc(data: &[u8]) -> u16 {
    // CRC lookup table from Java ChecksumCalculator
    const CONSTANTS: [u16; 16] = [
        0x0000, 0xCC01, 0xD801, 0x1400, 0xF001, 0x3C00, 0x2800, 0xE401, 0xA001, 0x6C00, 0x7800,
        0xB401, 0x5000, 0x9C01, 0x8801, 0x4400,
    ];

    let mut crc: u16 = 0;
    for &byte in data {
        let b = byte as u16;
        // Process low nibble
        crc = (((crc >> 4) & 0x0FFF) ^ CONSTANTS[(crc & 0x0F) as usize])
            ^ CONSTANTS[(b & 0x0F) as usize];
        // Process high nibble
        crc = (((crc >> 4) & 0x0FFF) ^ CONSTANTS[(crc & 0x0F) as usize])
            ^ CONSTANTS[((b >> 4) & 0x0F) as usize];
    }
    crc
}

// ============================================================================
// DBus Notification Handler
// ============================================================================

async fn handle_dbus_notification(
    msg: &Message,
    handler: &GarminNotificationHandler,
    filter_apps: &Option<Vec<String>>,
    min_urgency: u8,
    enable_dedup: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let header = msg.header();
    let member = header.member().ok_or("No member")?;
    let interface = header.interface().map(|i| i.as_str()).unwrap_or("");

    // Handle standard notifications, XDG Portal notifications, and GTK notifications
    let (app_name, summary, body_text, hints): (
        String,
        String,
        String,
        HashMap<String, zbus::zvariant::OwnedValue>,
    ) = if interface == "org.kde.NotificationWatcher" && member == "Notify" {
        // KDE notifications (used by KDE Plasma desktop)
        // Signature: "ususssasa{sv}i" - id, app_name, replaces_id, icon, summary, body, actions, hints, timeout
        let (_notification_id, app, _replaces_id, _icon, title, body, _actions, hints, _timeout): (
            u32,
            String,
            u32,
            String,
            String,
            String,
            Vec<String>,
            HashMap<String, zbus::zvariant::OwnedValue>,
            i32,
        ) = msg.body().deserialize()?;

        (app, title, body, hints)
    } else if (interface == "org.gtk.Notifications"
        || interface == "org.freedesktop.impl.portal.Notification")
        && member == "AddNotification"
    {
        // GTK notifications (used by many Flatpak apps like Fractal)
        // Also handles portal implementation interface (org.freedesktop.impl.portal.Notification)
        // Signature: "ssa{sv}" - app_id, notification_id, and properties dict
        let (app_id, _notification_id, properties): (
            String,
            String,
            HashMap<String, zbus::zvariant::OwnedValue>,
        ) = msg.body().deserialize()?;

        // Extract title and body from properties
        let title = properties
            .get("title")
            .and_then(|v| <&str>::try_from(v).ok())
            .unwrap_or("")
            .to_string();

        let body = properties
            .get("body")
            .and_then(|v| <&str>::try_from(v).ok())
            .unwrap_or("")
            .to_string();

        // Use app_id as the app name (e.g., "com.fastmail.Fastmail" or "org.gnome.Fractal")
        let app = if app_id.is_empty() {
            "GTK App".to_string()
        } else {
            // Extract readable name from reverse domain (e.g., "com.fastmail.Fastmail" -> "Fastmail")
            app_id.split('.').last().unwrap_or(&app_id).to_string()
        };

        // GTK notifications use different hint structure
        let mut converted_hints = HashMap::new();

        // Check for priority hint
        if let Some(priority) = properties.get("priority") {
            if let Ok(p) = <&str>::try_from(priority) {
                let urgency = match p {
                    "low" => 0u8,
                    "normal" => 1u8,
                    "high" | "urgent" => 2u8,
                    _ => 1u8,
                };
                converted_hints.insert(
                    "urgency".to_string(),
                    zbus::zvariant::Value::new(urgency).try_into().unwrap(),
                );
            }
        }

        // Add desktop-entry hint from app_id for better detection
        converted_hints.insert(
            "desktop-entry".to_string(),
            zbus::zvariant::Value::new(app_id.as_str())
                .try_into()
                .unwrap(),
        );

        // Add category hint for known app types
        if app_id.contains("fractal") || app_id.contains("matrix") {
            converted_hints.insert(
                "category".to_string(),
                zbus::zvariant::Value::new("im.received")
                    .try_into()
                    .unwrap(),
            );
        } else if app_id.contains("mail")
            || app_id.contains("geary")
            || app_id.contains("thunderbird")
        {
            converted_hints.insert(
                "category".to_string(),
                zbus::zvariant::Value::new("email.arrived")
                    .try_into()
                    .unwrap(),
            );
        }

        (app, title, body, converted_hints)
    } else if interface == "org.freedesktop.portal.Notification" && member == "AddNotification" {
        // XDG Desktop Portal notifications (used by Fractal, Flatpak apps, etc.)
        // Signature: "sa{sv}" - notification_id and properties dict
        let (notification_id, properties): (String, HashMap<String, zbus::zvariant::OwnedValue>) =
            msg.body().deserialize()?;

        // Extract title and body from properties
        let title = properties
            .get("title")
            .and_then(|v| <&str>::try_from(v).ok())
            .unwrap_or("")
            .to_string();

        let body = properties
            .get("body")
            .and_then(|v| <&str>::try_from(v).ok())
            .unwrap_or("")
            .to_string();

        // Portal notifications don't have traditional app_name, use a generic identifier
        // We'll detect the app from the sender or use "Portal App"
        let app = if notification_id.contains("matrix") || notification_id.contains("fractal") {
            "Fractal".to_string()
        } else {
            header
                .sender()
                .map(|s| s.as_str().to_string())
                .unwrap_or_else(|| "Portal App".to_string())
        };

        // Portal notifications use different hint structure
        let mut converted_hints = HashMap::new();

        // Check for priority hint
        if let Some(priority) = properties.get("priority") {
            if let Ok(p) = <&str>::try_from(priority) {
                let urgency = match p {
                    "low" => 0u8,
                    "normal" => 1u8,
                    "high" | "urgent" => 2u8,
                    _ => 1u8,
                };
                converted_hints.insert(
                    "urgency".to_string(),
                    zbus::zvariant::Value::new(urgency).try_into().unwrap(),
                );
            }
        }

        // Add category hint for Matrix/Fractal
        if notification_id.contains("matrix") {
            converted_hints.insert(
                "category".to_string(),
                zbus::zvariant::Value::new("im.received")
                    .try_into()
                    .unwrap(),
            );
            converted_hints.insert(
                "desktop-entry".to_string(),
                zbus::zvariant::Value::new("org.gnome.Fractal")
                    .try_into()
                    .unwrap(),
            );
        }

        (app, title, body, converted_hints)
    } else if member == "Notify" {
        // Standard freedesktop.org notifications
        // Signature: "susssasa{sv}i"
        let (app, _replaces_id, _app_icon, title, body, _actions, hints, _timeout): (
            String,
            u32,
            String,
            String,
            String,
            Vec<String>,
            HashMap<String, zbus::zvariant::OwnedValue>,
            i32,
        ) = msg.body().deserialize()?;

        (app, title, body, hints)
    } else {
        // Not a notification we handle
        return Ok(());
    };

    // Check urgency level (optional filter)
    if min_urgency > 0 {
        if let Some(urgency_value) = hints.get("urgency") {
            if let Ok(urgency) = u8::try_from(urgency_value) {
                if urgency < min_urgency {
                    println!(
                        "   ⏭️  Skipping low urgency notification (urgency={}, min={})",
                        urgency, min_urgency
                    );
                    return Ok(());
                }
            }
        }
    }

    // Check app filter (optional)
    if let Some(ref apps) = filter_apps {
        if !apps
            .iter()
            .any(|a| app_name.to_lowercase().contains(&a.to_lowercase()))
        {
            println!(
                "   ⏭️  Skipping notification (app '{}' not in filter)",
                app_name
            );
            return Ok(());
        }
    }

    // Allow empty summary - use app name as fallback
    let notification_summary = if summary.is_empty() {
        println!("   ℹ️  Empty summary, using app name as title");
        app_name.clone()
    } else {
        summary.clone()
    };

    // Decode HTML entities in body text (some apps like KMail send HTML entities)
    let decoded_body = body_text
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("<br/>", "\n")
        .replace("<br>", "\n");

    println!(
        "📱 Notification from {} [via {}]: {} - {}",
        app_name, interface, summary, decoded_body
    );

    // Log all available hints for debugging
    if !hints.is_empty() {
        println!("   🔍 Available hints:");
        for (key, value) in &hints {
            // Try to extract string value for common hint types
            let value_str = if let Ok(s) = <&str>::try_from(value) {
                format!("\"{}\"", s)
            } else if let Ok(b) = <bool>::try_from(value) {
                format!("{}", b)
            } else if let Ok(i) = <i32>::try_from(value) {
                format!("{}", i)
            } else if let Ok(u) = <u32>::try_from(value) {
                format!("{}", u)
            } else if let Ok(u) = <u8>::try_from(value) {
                format!("{}", u)
            } else {
                format!("<{}>", value.value_signature())
            };
            println!("      - {}: {}", key, value_str);
        }
    }

    // Optional: Check for duplicate content (within last 50 notifications)
    // Disabled by default to ensure all notifications pass through
    // Enable with --enable-dedup flag
    if enable_dedup {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        decoded_body.hash(&mut hasher);
        let body_hash = hasher.finish();

        // Dedup based on content only (title + body hash), regardless of app name or interface
        // Same notification can arrive via multiple interfaces with different app names
        // (e.g., ":1.4469" via portal.Notification, then "Fastmail" via impl.portal.Notification)
        let content_key = (notification_summary.clone(), body_hash);
        {
            let mut seen = handler.seen_dbus_messages.lock().unwrap();
            if seen.contains(&content_key) {
                println!(
                    "   ⏭️  Skipping duplicate notification (same content already sent, now via {})",
                    interface
                );
                return Ok(());
            }

            println!(
                "   🔍 Dedup check: New notification (hash: {:x}, via {})",
                body_hash, interface
            );
            seen.insert(content_key);

            // Cleanup old entries (keep last 5)
            if seen.len() > 5 {
                let keys_to_remove: Vec<_> = seen.iter().take(20).cloned().collect();
                for key in keys_to_remove {
                    seen.remove(&key);
                }
            }
        }
    }

    // Map application to notification type
    let notification_type = map_app_to_notification_type(&app_name, &hints);

    // Log notification type mapping for debugging
    println!(
        "   🏷️  Mapped to type: {:?} (category hint: {:?}, desktop-entry: {:?})",
        notification_type,
        hints.get("category").and_then(|v| <&str>::try_from(v).ok()),
        hints
            .get("desktop-entry")
            .and_then(|v| <&str>::try_from(v).ok())
    );

    // Generate notification ID
    let notification_id = handler.get_next_notification_id();

    // Create notification spec
    let spec = NotificationSpec::new(notification_id, notification_type)
        .with_title(notification_summary)
        .with_body(decoded_body)
        .with_sender(app_name);

    // Send to watch
    if let Err(e) = handler.on_notification(spec).await {
        eprintln!("   ❌ Failed to send notification: {}", e);
    } else {
        println!("   ✅ Sent to watch (ID: {})", notification_id);
    }

    Ok(())
}

async fn handle_modemmanager_call(
    msg: &zbus::Message,
    handler: &GarminNotificationHandler,
    system_connection: &zbus::Connection,
) -> Result<(), Box<dyn std::error::Error>> {
    // Parse PropertiesChanged signal: "sa{sv}as"
    let (interface_name, changed_properties, _invalidated): (
        String,
        HashMap<String, zbus::zvariant::OwnedValue>,
        Vec<String>,
    ) = msg.body().deserialize()?;

    // Check if this is a Voice interface change
    if interface_name != "org.freedesktop.ModemManager1.Modem.Voice" {
        return Ok(());
    }

    // Check if Calls array changed
    if let Some(calls_value) = changed_properties.get("Calls") {
        // Parse calls array
        let calls: Vec<zbus::zvariant::OwnedObjectPath> = if let Ok(v) = calls_value.try_clone() {
            match zbus::zvariant::Value::try_from(v) {
                Ok(value) => {
                    if let Some(array) =
                        <Vec<zbus::zvariant::OwnedObjectPath>>::try_from(value).ok()
                    {
                        array
                    } else {
                        Vec::new()
                    }
                }
                Err(_) => Vec::new(),
            }
        } else {
            Vec::new()
        };

        if !calls.is_empty() {
            println!("📞 ModemManager: Incoming call detected!");
            let call_path = calls[0].as_str();
            println!("   Call path: {}", call_path);

            // Query ModemManager for call details (phone number)
            let phone_number = match system_connection
                .call_method(
                    Some("org.freedesktop.ModemManager1"),
                    call_path,
                    Some("org.freedesktop.DBus.Properties"),
                    "Get",
                    &("org.freedesktop.ModemManager1.Call", "Number"),
                )
                .await
            {
                Ok(reply) => match reply.body().deserialize::<(zbus::zvariant::OwnedValue,)>() {
                    Ok((number_variant,)) => match <&str>::try_from(&number_variant) {
                        Ok(number) => {
                            println!("   📱 Caller ID: {}", number);
                            number.to_string()
                        }
                        Err(_) => {
                            println!("   ⚠️  Could not parse phone number");
                            "Unknown".to_string()
                        }
                    },
                    Err(_) => {
                        println!("   ⚠️  Could not get phone number from reply");
                        "Unknown".to_string()
                    }
                },
                Err(e) => {
                    eprintln!("   ⚠️  Failed to query call number: {}", e);
                    "Unknown".to_string()
                }
            };

            let call_spec = CallSpec::new(phone_number.clone(), CallCommand::Incoming)
                .with_name(phone_number.clone());

            println!("   📱 Sending incoming call notification to watch...");
            if let Err(e) = handler.on_set_call_state(call_spec).await {
                eprintln!("   ❌ Failed to send call notification: {}", e);
            } else {
                println!("   ✅ Call notification sent to watch!");
            }
        } else {
            println!("📞 ModemManager: Call ended");
            // Send call end notification
            let call_spec = CallSpec::new("Unknown".to_string(), CallCommand::End);
            let _ = handler.on_set_call_state(call_spec).await;
        }
    }

    Ok(())
}

fn map_app_to_notification_type(
    app_name: &str,
    hints: &HashMap<String, zbus::zvariant::OwnedValue>,
) -> NotificationType {
    let app_lower = app_name.to_lowercase();

    // Check for category hint first (freedesktop.org notification spec)
    if let Some(category) = hints.get("category") {
        if let Ok(cat_str) = <&str>::try_from(category) {
            let cat_lower = cat_str.to_lowercase();

            // Instant messaging / chat
            if cat_lower.contains("im") || cat_lower.contains("chat") {
                return NotificationType::GenericChat;
            }

            // Email
            if cat_lower.contains("email") {
                return NotificationType::GenericEmail;
            }

            // Social media / presence
            if cat_lower.contains("social") || cat_lower.contains("presence") {
                return NotificationType::GenericSocial;
            }

            // Calendar / schedule
            if cat_lower.contains("calendar") || cat_lower.contains("schedule") {
                return NotificationType::GenericCalendar;
            }

            // Navigation / location
            if cat_lower.contains("navigation") || cat_lower.contains("location") {
                return NotificationType::GenericNavigation;
            }

            // Alarms / timers
            if cat_lower.contains("alarm") || cat_lower.contains("timer") {
                return NotificationType::GenericAlarmClock;
            }
        }
    }

    // Check desktop-entry hint for better app identification
    if let Some(desktop_entry) = hints.get("desktop-entry") {
        if let Ok(entry_str) = <&str>::try_from(desktop_entry) {
            let entry_lower = entry_str.to_lowercase();

            // Chat apps
            if entry_lower.contains("telegram")
                || entry_lower.contains("signal")
                || entry_lower.contains("whatsapp")
                || entry_lower.contains("discord")
                || entry_lower.contains("slack")
                || entry_lower.contains("element")
                || entry_lower.contains("fractal")
                || entry_lower.contains("mattermost")
                || entry_lower.contains("teams")
                || entry_lower.contains("skype")
                || entry_lower.contains("zoom")
                || entry_lower.contains("messenger")
                || entry_lower.contains("viber")
                || entry_lower.contains("wire")
            {
                return NotificationType::GenericChat;
            }

            // Email apps
            if entry_lower.contains("mail")
                || entry_lower.contains("thunderbird")
                || entry_lower.contains("evolution")
                || entry_lower.contains("geary")
                || entry_lower.contains("outlook")
            {
                return NotificationType::GenericEmail;
            }

            // Social media
            if entry_lower.contains("twitter")
                || entry_lower.contains("mastodon")
                || entry_lower.contains("facebook")
                || entry_lower.contains("instagram")
                || entry_lower.contains("linkedin")
                || entry_lower.contains("reddit")
            {
                return NotificationType::GenericSocial;
            }

            // Calendar apps
            if entry_lower.contains("calendar")
                || entry_lower.contains("gnome-clocks")
                || entry_lower.contains("organizer")
            {
                return NotificationType::GenericCalendar;
            }
        }
    }

    // Map based on application name (fallback if no category/desktop-entry hints)

    // SMS apps
    if app_lower.contains("sms")
        || app_lower.contains("messages")
        || app_lower.contains("texting")
        || app_lower.contains("mmssms")
    {
        return NotificationType::GenericSms;
    }

    // Chat apps
    if app_lower.contains("telegram")
        || app_lower.contains("signal")
        || app_lower.contains("whatsapp")
        || app_lower.contains("discord")
        || app_lower.contains("slack")
        || app_lower.contains("element")
        || app_lower.contains("fractal")
        || app_lower.contains("matrix")
        || app_lower.contains("mattermost")
        || app_lower.contains("teams")
        || app_lower.contains("skype")
        || app_lower.contains("zoom")
        || app_lower.contains("messenger")
        || app_lower.contains("viber")
        || app_lower.contains("wire")
        || app_lower.contains("rocketchat")
        || app_lower.contains("jitsi")
        || app_lower.contains("riot")
        || app_lower.contains("chat")
    {
        return NotificationType::GenericChat;
    }

    // Email apps
    if app_lower.contains("mail")
        || app_lower.contains("thunderbird")
        || app_lower.contains("evolution")
        || app_lower.contains("geary")
        || app_lower.contains("outlook")
        || app_lower.contains("k9")
        || app_lower.contains("k-9")
        || app_lower.contains("aqua mail")
        || app_lower.contains("fairmail")
    {
        return NotificationType::GenericEmail;
    }

    // Social media apps
    if app_lower.contains("twitter")
        || app_lower.contains("mastodon")
        || app_lower.contains("facebook")
        || app_lower.contains("instagram")
        || app_lower.contains("linkedin")
        || app_lower.contains("reddit")
        || app_lower.contains("tumblr")
        || app_lower.contains("social")
        || app_lower.contains("tiktok")
        || app_lower.contains("snapchat")
    {
        return NotificationType::GenericSocial;
    }

    // Calendar and reminder apps
    if app_lower.contains("calendar")
        || app_lower.contains("remind")
        || app_lower.contains("task")
        || app_lower.contains("todo")
        || app_lower.contains("organizer")
        || app_lower.contains("planner")
        || app_lower.contains("schedule")
        || app_lower.contains("event")
    {
        return NotificationType::GenericCalendar;
    }

    // Alarm and timer apps
    if app_lower.contains("alarm")
        || app_lower.contains("timer")
        || app_lower.contains("clock")
        || app_lower.contains("stopwatch")
    {
        return NotificationType::GenericAlarmClock;
    }

    // Navigation apps
    if app_lower.contains("maps")
        || app_lower.contains("navigation")
        || app_lower.contains("waze")
        || app_lower.contains("osmand")
        || app_lower.contains("location")
        || app_lower.contains("gps")
    {
        return NotificationType::GenericNavigation;
    }

    // Default fallback
    NotificationType::Generic
}

// ============================================================================
// Main Application
// ============================================================================

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .filter_module("zbus::connection::socket_reader", log::LevelFilter::Warn)
        .init();

    let args = Args::parse();

    println!("🚀 Garmin DBus Notification Monitor with Watchdog\n");
    println!("Target Device: {}", args.mac_address);
    if let Some(ref filter) = args.filter_apps {
        println!("Filtering apps: {}", filter);
    }
    println!("Minimum urgency: {}", args.min_urgency);
    println!(
        "Duplicate detection: {}",
        if args.enable_dedup {
            "enabled"
        } else {
            "disabled (all notifications pass through)"
        }
    );
    println!(
        "📅 Calendar sync: {}",
        if args.enable_calendar_sync {
            format!(
                "enabled (every {} min, {} days lookahead)",
                args.calendar_sync_interval, args.calendar_lookahead_days
            )
        } else {
            "disabled".to_string()
        }
    );
    if let Some(ref urls) = args.calendar_urls {
        let url_count = urls.split(',').filter(|s| !s.trim().is_empty()).count();
        println!(
            "   📡 URL calendars: {} configured (cache: {}s)",
            url_count, args.calendar_cache_duration
        );
    }
    println!("🔄 Auto-reconnect: enabled");

    // Main reconnection loop (watchdog will handle most reconnections internally)
    let mut reconnect_attempt = 0;
    loop {
        if reconnect_attempt > 0 {
            println!("\n🔄 Manual reconnection attempt #{}", reconnect_attempt);
            sleep(Duration::from_secs(5)).await;
        }

        match run_monitor(&args).await {
            Ok(_) => {
                println!("\n✅ Monitor exited normally");
                break;
            }
            Err(e) => {
                reconnect_attempt += 1;
                eprintln!("\n❌ Monitor error: {}", e);
                eprintln!("🔄 Will attempt manual reconnect in 5 seconds...");

                if reconnect_attempt >= 10 {
                    eprintln!(
                        "\n❌ Too many reconnection attempts ({}), giving up",
                        reconnect_attempt
                    );
                    return Err(e);
                }
            }
        }
    }

    Ok(())
}

/// Connection result containing all components needed for watch communication
struct WatchConnection {
    ble_support: Arc<BlueRSupport>,
    communicator: Arc<CommunicatorV2>,
}

/// Connect to the watch and set up all communication channels
async fn connect_to_watch(
    adapter: &Adapter,
    mac_address: &str,
    watchdog: Arc<WatchdogManager>,
    async_handler: &Arc<AsyncMessageHandler>,
) -> std::result::Result<WatchConnection, Box<dyn std::error::Error>> {
    println!("\n🔌 Connecting to watch {}...", mac_address);

    // Connect to device
    let ble_support = match BlueRSupport::new(adapter, mac_address).await {
        Ok(support) => support,
        Err(e) => {
            println!("   ❌ Failed to connect: {}", e);
            return Err(e);
        }
    };
    println!("   ✅ Connected to device");

    let ble_arc = Arc::new(ble_support);

    // Discover characteristics
    println!("\n🔍 Discovering services and characteristics...");
    if let Err(e) = ble_arc.discover_services().await {
        println!("   ❌ Failed to discover services: {}", e);
        return Err(e);
    }

    // Create communicator
    println!("\n3️⃣  Creating communicator...");
    let mut communicator = CommunicatorV2::new(ble_arc.clone());
    println!("   ✅ Communicator created");

    // Set up async message handler BEFORE initialization
    communicator.set_async_message_callback(async_handler.clone());

    // Find characteristics
    println!("\n4️⃣  Finding Garmin ML characteristics...");
    let init_result = match communicator.initialize_device().await {
        Ok(true) => {
            println!("   ✅ Characteristics found and set up!");
            true
        }
        Ok(false) => {
            println!("   ❌ Failed to find Garmin ML characteristics");
            return Err(Box::new(std::io::Error::other(
                "Failed to find characteristics",
            )));
        }
        Err(e) => {
            println!("   ❌ Initialization error: {}", e);
            return Err(Box::new(e));
        }
    };

    if !init_result {
        return Err(Box::new(std::io::Error::other("Initialization failed")));
    }

    // Create Arc for communicator
    let communicator = Arc::new(communicator);

    // Set communicator reference in async handler
    async_handler.set_communicator(communicator.clone());

    // Start notification listener
    println!("\n5️⃣  Starting notification listener...");
    let receive_uuid = communicator.get_receive_characteristic_uuid().await;

    if let Some(uuid) = receive_uuid {
        println!("   Using receive characteristic: {}", &uuid[..20]);
        match ble_arc
            .start_notification_listener(&uuid, communicator.clone(), Some(watchdog.clone()))
            .await
        {
            Ok(_) => {
                println!("   ✅ Listener is active and ready to receive responses");
            }
            Err(e) => {
                println!("   ❌ Failed to start listener: {}", e);
                return Err(e);
            }
        }
    } else {
        println!("   ❌ No receive characteristic found");
        return Err(Box::new(std::io::Error::other(
            "No receive characteristic found",
        )));
    }

    // Small delay to ensure listener is fully settled
    sleep(Duration::from_millis(200)).await;

    // Send initialization requests
    println!("\n6️⃣  Sending initialization requests...");
    match communicator.initialize_device().await {
        Ok(true) => {
            println!("   ✅ CLOSE_ALL_REQ sent");
            println!("   ℹ️  Waiting for watch responses...");
        }
        Ok(false) => {
            println!("   ⚠️  Re-initialization returned false");
        }
        Err(e) => {
            println!("   ❌ Initialization error: {}", e);
            return Err(Box::new(e));
        }
    }

    // Wait for MLR registration to complete
    sleep(Duration::from_secs(3)).await;
    println!("   ✅ MLR registration should be complete");

    // Handle pairing if needed
    println!("\n✅ Watch connection fully established!\n");

    Ok(WatchConnection {
        ble_support: ble_arc,
        communicator,
    })
}

async fn run_monitor(args: &Args) -> std::result::Result<(), Box<dyn std::error::Error>> {
    // Create watchdog FIRST - before connection
    println!("🛡️  Creating connection watchdog...");
    let mut watchdog_config = WatchdogConfig::default();
    watchdog_config.rx_timeout = Duration::from_secs(60 * 60); // one hour timeout
    let watchdog = Arc::new(WatchdogManager::with_config(watchdog_config));
    println!("   ✅ Watchdog configured (one hour timeout)");

    // Initialize Bluetooth
    println!("\n1️⃣  Initializing Bluetooth...");
    let session = match Session::new().await {
        Ok(s) => s,
        Err(e) => {
            println!("   ❌ Failed to initialize Bluetooth session: {}", e);
            return Err(Box::new(std::io::Error::other(
                "Failed to initialize Bluetooth session",
            )));
        }
    };
    let adapter = match session.default_adapter().await {
        Ok(a) => a,
        Err(e) => {
            error!("   ❌ Failed to get Bluetooth adapter: {}", e);
            return Err(Box::new(std::io::Error::other(
                "Failed to get Bluetooth adapter",
            )));
        }
    };
    let adapter_name = adapter.name();
    debug!("   Using adapter: {}", adapter_name);

    // Ensure Bluetooth adapter is powered on
    match adapter.is_powered().await {
        Ok(true) => {
            debug!("   ✅ Bluetooth adapter is powered on");
        }
        Ok(false) => {
            debug!("   ⚠️  Bluetooth adapter is off, attempting to turn it on...");

            // First try: Use rfkill to unblock Bluetooth
            debug!("   🔧 Attempting: rfkill unblock bluetooth");
            match std::process::Command::new("rfkill")
                .args(&["unblock", "bluetooth"])
                .output()
            {
                Ok(output) => {
                    if output.status.success() {
                        info!("   ✅ rfkill unblock bluetooth succeeded");
                        sleep(Duration::from_secs(5)).await;
                    } else {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        error!("   ⚠️  rfkill command failed: {}", stderr);
                    }
                }
                Err(e) => {
                    error!("   ⚠️  Could not run rfkill: {}", e);
                }
            }

            // Second try: Use bluetoothctl to unblock Bluetooth
            debug!("   🔧 Attempting: bluetoothctl power on");
            match std::process::Command::new("bluetoothctl")
                .args(&["power", "on"])
                .output()
            {
                Ok(output) => {
                    if output.status.success() {
                        info!("   ✅ bluetoothctl power on succeeded");
                        sleep(Duration::from_secs(5)).await;
                    } else {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        error!("   ⚠️  bluetoothctl command failed: {}", stderr);
                    }
                }
                Err(e) => {
                    error!("   ⚠️  Could not run bluetoothctl: {}", e);
                }
            }

            // Third try: Use adapter API to power on
            match adapter.set_powered(true).await {
                Ok(_) => {
                    // Wait a moment for adapter to fully power on
                    sleep(Duration::from_secs(2)).await;
                    info!("   ✅ Bluetooth adapter powered on");
                }
                Err(e) => {
                    error!("   ❌ Failed to power on Bluetooth adapter: {}", e);
                    return Err(Box::new(std::io::Error::other(
                        "Bluetooth adapter is off and could not be powered on automatically",
                    )));
                }
            }
        }
        Err(e) => {
            info!("   ⚠️  Could not check Bluetooth power state: {}", e);
            info!("   Continuing anyway - if connection fails, ensure Bluetooth is enabled");
        }
    }

    // Connect to device
    info!("\n2️⃣  Connecting to Garmin device...");
    let ble_support = match BlueRSupport::new(&adapter, &args.mac_address).await {
        Ok(support) => support,
        Err(e) => {
            error!("   ❌ Failed to connect: {}", e);
            return Err(Box::new(std::io::Error::other("Failed to connect")));
        }
    };

    // Mark watchdog as connected now - we have a BLE connection
    watchdog.mark_connected().await;
    println!("   ✅ Watchdog activated (monitoring from start)");

    // Discover services
    println!("\n3️⃣  Discovering services and characteristics...");
    if let Err(e) = ble_support.discover_services().await {
        println!("   ❌ Failed to discover services: {}", e);
        return Err(Box::new(std::io::Error::other(
            "Failed to discover services",
        )));
    }

    // Create communicator
    println!("\n4️⃣  Creating communicator...");
    let ble_arc = Arc::new(ble_support);
    let mut communicator = CommunicatorV2::new(ble_arc.clone());

    // Set up async message handler BEFORE initialization
    let async_handler = Arc::new(AsyncMessageHandler::new());
    communicator.set_async_message_callback(async_handler.clone());

    // Initialize calendar manager early if calendar sync is enabled
    // This must happen BEFORE the watch connects to avoid race conditions
    if args.enable_calendar_sync {
        println!("\n4.5️⃣  Initializing calendar manager...");

        // Start with default providers (GNOME Calendar, KOrganizer)
        let mut manager_result = CalendarManager::new().await;

        // If URL calendars are specified, add URL provider
        if let Some(urls_str) = &args.calendar_urls {
            let urls: Vec<String> = urls_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            if !urls.is_empty() {
                println!("   📡 Adding {} URL calendar(s)...", urls.len());
                for url in &urls {
                    println!("      - {}", url);
                }

                match &mut manager_result {
                    Ok(manager) => {
                        // Add URL provider to existing manager
                        if let Err(e) = manager.add_url_provider(urls, args.calendar_cache_duration)
                        {
                            eprintln!("   ⚠️  Failed to add URL calendar provider: {}", e);
                        } else {
                            println!(
                                "   ✅ URL calendar provider added (cache: {}s)",
                                args.calendar_cache_duration
                            );
                        }
                    }
                    Err(_) => {
                        // No local providers available, try just URL provider
                        use crate::calendar::UrlCalendarProvider;
                        match UrlCalendarProvider::new(urls, args.calendar_cache_duration) {
                            Ok(provider) => {
                                match CalendarManager::with_providers(vec![Box::new(provider)]) {
                                    Ok(mgr) => {
                                        manager_result = Ok(mgr);
                                        println!(
                                            "   ✅ URL calendar provider added (cache: {}s)",
                                            args.calendar_cache_duration
                                        );
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "   ⚠️  Failed to create manager with URL provider: {}",
                                            e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("   ⚠️  Failed to create URL calendar provider: {}", e);
                            }
                        }
                    }
                }
            }
        }

        match manager_result {
            Ok(manager) => {
                println!(
                    "   ✅ Calendar manager initialized with {} provider(s)",
                    manager.provider_count()
                );
                let manager_arc = Arc::new(manager);
                async_handler.set_calendar_manager(manager_arc.clone());
                println!("   ✅ Calendar manager registered (ready for watch requests)");
            }
            Err(e) => {
                eprintln!("   ⚠️  Failed to initialize calendar manager: {}", e);
                eprintln!("   ℹ️  Calendar sync will be disabled");
                eprintln!("   💡 Tip: Install GNOME Calendar/KOrganizer or use --calendar-urls");
            }
        }
    }

    // Connect to watch
    println!("\n2️⃣  Connecting to watch...");
    let connection = connect_to_watch(
        &adapter,
        &args.mac_address,
        watchdog.clone(),
        &async_handler,
    )
    .await?;

    let mut ble_arc = connection.ble_support;
    let mut communicator = connection.communicator;

    // Create notification handler
    println!("\n8️⃣  Creating notification handler...");
    let handler = Arc::new(GarminNotificationHandler::new(
        communicator.clone(),
        watchdog.clone(),
    ));
    println!("   ✅ Handler ready with watchdog protection");

    // Set the handler in the message callback
    async_handler.set_notification_handler(handler.clone());
    println!("   ✅ Handler registered with message callback");

    // Set up DBus monitoring
    println!("\n🔟 Setting up DBus notification monitor...");

    // Parse filter apps if provided
    let filter_apps: Option<Vec<String>> = args
        .filter_apps
        .as_ref()
        .map(|s| s.split(',').map(|a| a.trim().to_string()).collect());

    // Connect to session bus
    let connection = Connection::session().await?;
    println!("   ✅ Connected to DBus session bus");

    // Become a monitor for notifications
    println!("   📡 Monitoring org.freedesktop.Notifications...");

    // Use BecomeMonitor to intercept all notification method calls
    // We need to monitor multiple interfaces to catch all notification types
    let monitoring_proxy = zbus::fdo::MonitoringProxy::new(&connection).await?;

    // Create match rules for all notification interfaces
    let standard_rule = zbus::MatchRule::builder()
        .msg_type(zbus::message::Type::MethodCall)
        .interface("org.freedesktop.Notifications")?
        .member("Notify")?
        .build();

    let gtk_rule = zbus::MatchRule::builder()
        .msg_type(zbus::message::Type::MethodCall)
        .interface("org.gtk.Notifications")?
        .member("AddNotification")?
        .build();

    let portal_rule = zbus::MatchRule::builder()
        .msg_type(zbus::message::Type::MethodCall)
        .interface("org.freedesktop.portal.Notification")?
        .member("AddNotification")?
        .build();

    let portal_impl_rule = zbus::MatchRule::builder()
        .msg_type(zbus::message::Type::MethodCall)
        .interface("org.freedesktop.impl.portal.Notification")?
        .member("AddNotification")?
        .build();

    let kde_rule = zbus::MatchRule::builder()
        .msg_type(zbus::message::Type::MethodCall)
        .interface("org.kde.NotificationWatcher")?
        .member("Notify")?
        .build();

    let match_rules = vec![
        standard_rule,
        gtk_rule,
        portal_rule,
        portal_impl_rule,
        kde_rule,
    ];

    match monitoring_proxy.become_monitor(&match_rules, 0).await {
        Ok(_) => {
            println!("   ✅ DBus monitor active (monitoring all 5 notification interfaces)");
        }
        Err(e) => {
            eprintln!("   ❌ Failed to become DBus monitor: {}", e);
            eprintln!("   ℹ️  This may require special permissions.");
            eprintln!("   ℹ️  Trying fallback method (message stream without BecomeMonitor)...");
            // Don't return error - we'll try to continue with regular message stream
        }
    }

    // Variables for watchdog reconnection in main loop
    let mut last_watchdog_check = std::time::Instant::now();

    println!("\n✨ Monitor ready! Listening for desktop notifications...");
    println!("   All notifications will be forwarded to your Garmin watch.");
    println!("   🛡️  Watchdog: Auto-reconnect on connection loss");
    println!("   📥 Missed notifications will be queued and replayed on reconnect");
    if args.enable_calendar_sync {
        println!(
            "   📅 Calendar sync: Enabled (checking every {} minutes)",
            args.calendar_sync_interval
        );
    }
    println!("   Press Ctrl+C to exit.");
    println!();

    let handler_clone = handler.clone();
    let min_urgency = args.min_urgency;
    let enable_dedup = args.enable_dedup;

    println!("📡 Listening for notifications on DBus session bus...");
    println!("   ✅ Standard notifications (org.freedesktop.Notifications)");
    println!("   ✅ Portal notifications (org.freedesktop.portal.Notification)");
    println!("   ✅ Portal impl notifications (org.freedesktop.impl.portal.Notification)");
    println!("   ✅ GTK notifications (org.gtk.Notifications)");
    println!("   ✅ KDE notifications (org.kde.NotificationWatcher)");
    println!("   ✅ Phone calls (ModemManager on system bus)");
    println!(
        "   → Fractal, Flatpak apps, GTK apps, KDE apps, and phone calls now fully supported!\n"
    );

    // Also connect to system bus for ModemManager (phone calls)
    println!("📡 Connecting to system bus for phone call monitoring...");
    let system_connection = match zbus::Connection::system().await {
        Ok(conn) => {
            println!("   ✅ System bus connected (ModemManager)");

            // Add match rule to receive ModemManager signals
            // This tells DBus to send us PropertiesChanged signals from ModemManager
            match conn.call_method(
                Some("org.freedesktop.DBus"),
                "/org/freedesktop/DBus",
                Some("org.freedesktop.DBus"),
                "AddMatch",
                &("type='signal',interface='org.freedesktop.DBus.Properties',path_namespace='/org/freedesktop/ModemManager1'"),
            ).await {
                Ok(_) => {
                    println!("   ✅ Subscribed to ModemManager signals");
                    println!("   ℹ️  Listening for incoming call notifications...");
                }
                Err(e) => {
                    eprintln!("   ⚠️  Failed to subscribe to ModemManager signals: {}", e);
                    eprintln!("   ℹ️  Phone call notifications may not work");
                }
            }

            Some(conn)
        }
        Err(e) => {
            eprintln!("   ⚠️  Failed to connect to system bus: {}", e);
            eprintln!("   ℹ️  Phone call notifications (ModemManager) will not work");
            None
        }
    };

    let mut system_stream = if let Some(ref conn) = system_connection {
        Some(zbus::MessageStream::from(conn))
    } else {
        None
    };

    // Start listening for messages
    let mut stream = zbus::MessageStream::from(&connection);
    let mut last_metrics = std::time::Instant::now();
    let mut last_calendar_sync = std::time::Instant::now();

    // Get calendar manager reference (already initialized earlier)
    let calendar_manager = if args.enable_calendar_sync {
        async_handler.calendar_manager.lock().unwrap().clone()
    } else {
        None
    };

    loop {
        tokio::select! {
            Some(msg) = stream.next() => {
                if let Ok(msg) = msg {
                    if msg.message_type() == zbus::message::Type::MethodCall {
                        let header = msg.header();
                        let interface = header.interface().map(|i| i.as_str()).unwrap_or("<none>");
                        let member = header.member().map(|m| m.as_str()).unwrap_or("<none>");

                        // Only process notification-related method calls
                        if (interface == "org.freedesktop.Notifications" && member == "Notify") ||
                           (interface == "org.freedesktop.portal.Notification" && member == "AddNotification") ||
                           (interface == "org.freedesktop.impl.portal.Notification" && member == "AddNotification") ||
                           (interface == "org.gtk.Notifications" && member == "AddNotification") ||
                           (interface == "org.kde.NotificationWatcher" && member == "Notify") {
                            if let Err(e) = handle_dbus_notification(&msg, &handler_clone, &filter_apps, min_urgency, enable_dedup).await {
                                eprintln!("⚠️  Error handling notification: {}", e);
                            }
                        }
                    }
                }
            }
            Some(msg) = async {
                match &mut system_stream {
                    Some(stream) => stream.next().await,
                    None => None,
                }
            } => {
                if let Ok(msg) = msg {
                    if msg.message_type() == zbus::message::Type::Signal {
                        let header = msg.header();
                        let interface = header.interface().map(|i| i.as_str()).unwrap_or("<none>");
                        let member = header.member().map(|m| m.as_str()).unwrap_or("<none>");
                        let path = header.path().map(|p| p.as_str()).unwrap_or("<none>");

                        // Handle ModemManager call signals
                        if interface == "org.freedesktop.DBus.Properties"
                            && member == "PropertiesChanged"
                            && path.contains("ModemManager1/Modem") {

                            if let Some(ref sys_conn) = system_connection {
                                if let Err(e) = handle_modemmanager_call(&msg, &handler_clone, sys_conn).await {
                                    eprintln!("⚠️  Error handling ModemManager call: {}", e);
                                }
                            }
                        }
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(300)) => {
                // Show watchdog metrics every 5 minutes
                if last_metrics.elapsed() >= Duration::from_secs(300) {
                    let metrics = watchdog.get_metrics().await;
                    println!("   💓 Watchdog: {} | RX: {:?} ago | Reconnects: {}",
                        metrics.health_status,
                        metrics.last_rx_elapsed,
                        metrics.total_reconnects
                    );
                    last_metrics = std::time::Instant::now();
                }

                // Check if it's time to sync calendar
                if let Some(ref manager) = calendar_manager {
                    let sync_interval = Duration::from_secs(args.calendar_sync_interval * 60);
                    if last_calendar_sync.elapsed() >= sync_interval {
                        println!("📅 Syncing calendar events...");

                        let now = chrono::Utc::now().timestamp();
                        let lookahead = args.calendar_lookahead_days * 86400;
                        let end_time = now + lookahead;

                        match manager.fetch_events(now, end_time, 100, true).await {
                            Ok(events) => {
                                println!("   ✅ Fetched {} calendar events", events.len());

                                if !events.is_empty() {
                                    println!("   📋 Upcoming events:");
                                    for (i, event) in events.iter().take(5).enumerate() {
                                        let start_time = chrono::DateTime::from_timestamp(event.start_timestamp, 0)
                                            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
                                            .unwrap_or_else(|| "Unknown".to_string());
                                        println!("      {}. {} - {}", i + 1, event.title, start_time);
                                    }
                                    if events.len() > 5 {
                                        println!("      ... and {} more", events.len() - 5);
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("   ⚠️  Failed to fetch calendar events: {}", e);
                            }
                        }

                        last_calendar_sync = std::time::Instant::now();
                    }
                }
            }

            // Watchdog check every 15 seconds
            _ = tokio::time::sleep(Duration::from_secs(15)) => {
                if last_watchdog_check.elapsed() >= Duration::from_secs(15) {
                    last_watchdog_check = std::time::Instant::now();

                    if let Some(reason) = watchdog.check_reconnect_needed_sync(&ble_arc.device).await {
                        info!("   📡 Will attempt reconnection due to {reason}...");

                        // Mark watchdog as reconnecting
                        watchdog.start_reconnecting().await;
                        handler.set_connected(false);

                        // Cleanup old connection
                        communicator.clear_and_pause_mlr().await;
                        communicator.dispose().await;
                        ble_arc.stop_listener();

                        // Attempt reconnection
                        debug!("\n🔄 Attempting to reconnect to watch {}...", args.mac_address);

                        let mut attempt = 0;

                        'reconnect: loop {
                            attempt += 1;
                            info!(
                                "   🔄 Reconnection attempt {}...",
                                attempt,
                            );

                            if attempt > 0 && attempt % 20 == 0 {
                                info!("   🔌 Disconnecting old connection...");
                                if let Ok(true) = ble_arc.device.is_connected().await {
                                    if let Err(e) = ble_arc.device.disconnect().await {
                                        error!("   ⚠️  Disconnect error: {}", e);
                                    } else {
                                        debug!("   ✅ Disconnected cleanly");
                                    }
                                }
                            }

                            sleep(Duration::from_secs(2)).await;

                            watchdog.mark_disconnected().await;

                            // Ensure Bluetooth is powered on
                            match adapter.is_powered().await {
                                Ok(false) => {
                                    if let Ok(output) = std::process::Command::new("rfkill")
                                        .args(&["unblock", "bluetooth"])
                                        .output()
                                    {
                                        if !output.status.success() {
                                            error!("   ⚠️  rfkill failed");
                                            sleep(Duration::from_secs(120)).await;
                                            continue;
                                        }
                                    }

                                    sleep(Duration::from_secs(1)).await;

                                    // Second try: Use bluetoothctl to unblock Bluetooth
                                    debug!("   🔧 Attempting: bluetoothctl power on");
                                    match std::process::Command::new("bluetoothctl")
                                        .args(&["power", "on"])
                                        .output()
                                    {
                                        Ok(output) => {
                                            if output.status.success() {
                                                info!("   ✅ bluetoothctl power on succeeded");
                                            } else {
                                                let stderr = String::from_utf8_lossy(&output.stderr);
                                                error!("   ⚠️  bluetoothctl command failed: {}", stderr);
                                                sleep(Duration::from_secs(120)).await;
                                                continue;
                                            }
                                        }
                                        Err(e) => {
                                            error!("   ⚠️  Could not run bluetoothctl: {}", e);
                                            sleep(Duration::from_secs(120)).await;
                                            continue;
                                        }
                                    }

                                    sleep(Duration::from_secs(5)).await;
                                }
                                Err(e) => {
                                    error!("   ⚠️  Could not check Bluetooth power: {}", e);
                                    sleep(Duration::from_secs(120)).await;
                                    continue;
                                }
                                _ => {}
                            }

                            // Attempt connection
                            let reconnect_success = {
                                match connect_to_watch(
                                    &adapter,
                                    &args.mac_address,
                                    watchdog.clone(),
                                    &async_handler,
                                )
                                .await
                                {
                                    Ok(connection) => {
                                        info!("   ✅ Successfully reconnected!");

                                        drop(ble_arc);
                                        drop(communicator);

                                        // Update all references
                                        ble_arc = connection.ble_support.clone();
                                        communicator = connection.communicator.clone();

                                        handler.set_communicator(communicator.clone()).await;

                                        Some(())
                                    }
                                    Err(_) => {
                                        error!("   ❌ Connection failed");
                                        None
                                    }
                                }
                            };

                            if let Some(()) = reconnect_success {
                                sleep(Duration::from_secs(4)).await;

                                let health = watchdog.check_health().await;
                                info!("Watchdog Health: {:?}", health);

                                if health != HealthStatus::Healthy {
                                    continue;
                                }

                                // Mark as connected
                                watchdog.mark_connected().await;
                                handler.set_connected(true);
                                watchdog.finish_reconnecting().await;

                                // Replay missed notifications
                                if let Err(_) = handler.replay_missed_notifications().await {
                                    error!("   ⚠️  Error replaying notifications");
                                }

                                info!("   ✅ Reconnection complete! Resuming normal operation.\n");
                                break 'reconnect;
                            } else {
                                sleep(Duration::from_secs(40)).await;
                            }
                        }
                    }
                }
            }
        }
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_hints(
        category: Option<&str>,
        desktop_entry: Option<&str>,
    ) -> HashMap<String, zbus::zvariant::OwnedValue> {
        let mut hints = HashMap::new();
        if let Some(cat) = category {
            hints.insert(
                "category".to_string(),
                zbus::zvariant::Value::new(cat).try_into().unwrap(),
            );
        }
        if let Some(entry) = desktop_entry {
            hints.insert(
                "desktop-entry".to_string(),
                zbus::zvariant::Value::new(entry).try_into().unwrap(),
            );
        }
        hints
    }

    #[test]
    fn test_map_notification_type_category_hints() {
        // Test category hint for chat
        let hints = make_hints(Some("im"), None);
        assert!(matches!(
            map_app_to_notification_type("Unknown App", &hints),
            NotificationType::GenericChat
        ));

        let hints = make_hints(Some("im.received"), None);
        assert!(matches!(
            map_app_to_notification_type("Unknown App", &hints),
            NotificationType::GenericChat
        ));

        // Test category hint for email
        let hints = make_hints(Some("email"), None);
        assert!(matches!(
            map_app_to_notification_type("Unknown App", &hints),
            NotificationType::GenericEmail
        ));

        let hints = make_hints(Some("email.arrived"), None);
        assert!(matches!(
            map_app_to_notification_type("Unknown App", &hints),
            NotificationType::GenericEmail
        ));

        // Test category hint for social
        let hints = make_hints(Some("social"), None);
        assert!(matches!(
            map_app_to_notification_type("Unknown App", &hints),
            NotificationType::GenericSocial
        ));

        // Test category hint for calendar
        let hints = make_hints(Some("calendar"), None);
        assert!(matches!(
            map_app_to_notification_type("Unknown App", &hints),
            NotificationType::GenericCalendar
        ));

        // Test category hint for navigation
        let hints = make_hints(Some("navigation"), None);
        assert!(matches!(
            map_app_to_notification_type("Unknown App", &hints),
            NotificationType::GenericNavigation
        ));

        // Test category hint for alarm
        let hints = make_hints(Some("alarm"), None);
        assert!(matches!(
            map_app_to_notification_type("Unknown App", &hints),
            NotificationType::GenericAlarmClock
        ));
    }

    #[test]
    fn test_map_notification_type_desktop_entry_chat() {
        let hints = make_hints(None, Some("org.telegram.desktop"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericChat
        ));

        let hints = make_hints(None, Some("signal-desktop"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericChat
        ));

        let hints = make_hints(None, Some("whatsapp"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericChat
        ));

        let hints = make_hints(None, Some("discord"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericChat
        ));

        let hints = make_hints(None, Some("slack"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericChat
        ));

        let hints = make_hints(None, Some("element"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericChat
        ));

        let hints = make_hints(None, Some("org.gnome.Fractal"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericChat
        ));

        let hints = make_hints(None, Some("mattermost"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericChat
        ));

        let hints = make_hints(None, Some("teams"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericChat
        ));
    }

    #[test]
    fn test_map_notification_type_desktop_entry_email() {
        let hints = make_hints(None, Some("thunderbird"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericEmail
        ));

        let hints = make_hints(None, Some("evolution"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericEmail
        ));

        let hints = make_hints(None, Some("geary"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericEmail
        ));

        let hints = make_hints(None, Some("org.gnome.evolution"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericEmail
        ));
    }

    #[test]
    fn test_map_notification_type_desktop_entry_social() {
        let hints = make_hints(None, Some("twitter"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericSocial
        ));

        let hints = make_hints(None, Some("mastodon"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericSocial
        ));

        let hints = make_hints(None, Some("facebook"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericSocial
        ));

        let hints = make_hints(None, Some("reddit"));
        assert!(matches!(
            map_app_to_notification_type("Unknown", &hints),
            NotificationType::GenericSocial
        ));
    }

    #[test]
    fn test_map_notification_type_app_name_chat() {
        let hints = HashMap::new();

        assert!(matches!(
            map_app_to_notification_type("Telegram", &hints),
            NotificationType::GenericChat
        ));

        assert!(matches!(
            map_app_to_notification_type("Signal", &hints),
            NotificationType::GenericChat
        ));

        assert!(matches!(
            map_app_to_notification_type("WhatsApp", &hints),
            NotificationType::GenericChat
        ));

        assert!(matches!(
            map_app_to_notification_type("Discord", &hints),
            NotificationType::GenericChat
        ));

        assert!(matches!(
            map_app_to_notification_type("Slack", &hints),
            NotificationType::GenericChat
        ));

        assert!(matches!(
            map_app_to_notification_type("Element", &hints),
            NotificationType::GenericChat
        ));

        assert!(matches!(
            map_app_to_notification_type("Matrix Chat", &hints),
            NotificationType::GenericChat
        ));

        assert!(matches!(
            map_app_to_notification_type("Fractal", &hints),
            NotificationType::GenericChat
        ));

        assert!(matches!(
            map_app_to_notification_type("Microsoft Teams", &hints),
            NotificationType::GenericChat
        ));

        assert!(matches!(
            map_app_to_notification_type("Zoom", &hints),
            NotificationType::GenericChat
        ));

        assert!(matches!(
            map_app_to_notification_type("Messenger", &hints),
            NotificationType::GenericChat
        ));
    }

    #[test]
    fn test_map_notification_type_app_name_sms() {
        let hints = HashMap::new();

        assert!(matches!(
            map_app_to_notification_type("SMS", &hints),
            NotificationType::GenericSms
        ));

        assert!(matches!(
            map_app_to_notification_type("Messages", &hints),
            NotificationType::GenericSms
        ));

        assert!(matches!(
            map_app_to_notification_type("Texting App", &hints),
            NotificationType::GenericSms
        ));
    }

    #[test]
    fn test_map_notification_type_app_name_email() {
        let hints = HashMap::new();

        assert!(matches!(
            map_app_to_notification_type("Thunderbird", &hints),
            NotificationType::GenericEmail
        ));

        assert!(matches!(
            map_app_to_notification_type("Evolution", &hints),
            NotificationType::GenericEmail
        ));

        assert!(matches!(
            map_app_to_notification_type("Geary", &hints),
            NotificationType::GenericEmail
        ));

        assert!(matches!(
            map_app_to_notification_type("Gmail", &hints),
            NotificationType::GenericEmail
        ));

        assert!(matches!(
            map_app_to_notification_type("K-9 Mail", &hints),
            NotificationType::GenericEmail
        ));

        assert!(matches!(
            map_app_to_notification_type("FairMail", &hints),
            NotificationType::GenericEmail
        ));
    }

    #[test]
    fn test_map_notification_type_app_name_social() {
        let hints = HashMap::new();

        assert!(matches!(
            map_app_to_notification_type("Twitter", &hints),
            NotificationType::GenericSocial
        ));

        assert!(matches!(
            map_app_to_notification_type("Mastodon", &hints),
            NotificationType::GenericSocial
        ));

        assert!(matches!(
            map_app_to_notification_type("Facebook", &hints),
            NotificationType::GenericSocial
        ));

        assert!(matches!(
            map_app_to_notification_type("Instagram", &hints),
            NotificationType::GenericSocial
        ));

        assert!(matches!(
            map_app_to_notification_type("LinkedIn", &hints),
            NotificationType::GenericSocial
        ));

        assert!(matches!(
            map_app_to_notification_type("Reddit", &hints),
            NotificationType::GenericSocial
        ));

        assert!(matches!(
            map_app_to_notification_type("TikTok", &hints),
            NotificationType::GenericSocial
        ));
    }

    #[test]
    fn test_map_notification_type_app_name_calendar() {
        let hints = HashMap::new();

        assert!(matches!(
            map_app_to_notification_type("Calendar", &hints),
            NotificationType::GenericCalendar
        ));

        assert!(matches!(
            map_app_to_notification_type("Google Calendar", &hints),
            NotificationType::GenericCalendar
        ));

        assert!(matches!(
            map_app_to_notification_type("Reminder", &hints),
            NotificationType::GenericCalendar
        ));

        assert!(matches!(
            map_app_to_notification_type("Tasks", &hints),
            NotificationType::GenericCalendar
        ));

        assert!(matches!(
            map_app_to_notification_type("Todo List", &hints),
            NotificationType::GenericCalendar
        ));

        assert!(matches!(
            map_app_to_notification_type("Event Planner", &hints),
            NotificationType::GenericCalendar
        ));
    }

    #[test]
    fn test_map_notification_type_app_name_alarm() {
        let hints = HashMap::new();

        assert!(matches!(
            map_app_to_notification_type("Alarm", &hints),
            NotificationType::GenericAlarmClock
        ));

        assert!(matches!(
            map_app_to_notification_type("Timer", &hints),
            NotificationType::GenericAlarmClock
        ));

        assert!(matches!(
            map_app_to_notification_type("Clock", &hints),
            NotificationType::GenericAlarmClock
        ));

        assert!(matches!(
            map_app_to_notification_type("Stopwatch", &hints),
            NotificationType::GenericAlarmClock
        ));
    }

    #[test]
    fn test_map_notification_type_app_name_navigation() {
        let hints = HashMap::new();

        assert!(matches!(
            map_app_to_notification_type("Google Maps", &hints),
            NotificationType::GenericNavigation
        ));

        assert!(matches!(
            map_app_to_notification_type("Navigation", &hints),
            NotificationType::GenericNavigation
        ));

        assert!(matches!(
            map_app_to_notification_type("Waze", &hints),
            NotificationType::GenericNavigation
        ));

        assert!(matches!(
            map_app_to_notification_type("OsmAnd", &hints),
            NotificationType::GenericNavigation
        ));

        assert!(matches!(
            map_app_to_notification_type("GPS Navigation", &hints),
            NotificationType::GenericNavigation
        ));
    }

    #[test]
    fn test_map_notification_type_fallback() {
        let hints = HashMap::new();

        // Unknown app with no hints should fall back to Generic
        assert!(matches!(
            map_app_to_notification_type("Unknown App", &hints),
            NotificationType::Generic
        ));

        assert!(matches!(
            map_app_to_notification_type("Some Random App", &hints),
            NotificationType::Generic
        ));
    }

    #[test]
    fn test_map_notification_type_priority_category_over_app_name() {
        // Category hint should take priority over app name
        let hints = make_hints(Some("email"), None);

        // Even though app name suggests chat, category should win
        assert!(matches!(
            map_app_to_notification_type("Telegram", &hints),
            NotificationType::GenericEmail
        ));
    }

    #[test]
    fn test_map_notification_type_priority_desktop_entry_over_app_name() {
        // Desktop entry should take priority over app name
        let hints = make_hints(None, Some("thunderbird"));

        // Even if app name doesn't clearly indicate email
        assert!(matches!(
            map_app_to_notification_type("Some App", &hints),
            NotificationType::GenericEmail
        ));
    }

    #[test]
    fn test_map_notification_type_case_insensitive() {
        let hints = HashMap::new();

        // Test case insensitivity
        assert!(matches!(
            map_app_to_notification_type("TELEGRAM", &hints),
            NotificationType::GenericChat
        ));

        assert!(matches!(
            map_app_to_notification_type("TeLEgRaM", &hints),
            NotificationType::GenericChat
        ));

        assert!(matches!(
            map_app_to_notification_type("telegram", &hints),
            NotificationType::GenericChat
        ));
    }
}
