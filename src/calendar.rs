//! Calendar integration for GNOME Calendar (Evolution Data Server) and KOrganizer (Akonadi)
//!
//! This module provides functionality to read calendar events from Linux desktop calendar
//! applications and format them for synchronization with Garmin watches.

use chrono::{Datelike, Local, TimeZone, Timelike, Utc};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a calendar event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CalendarEvent {
    /// Unique identifier for the event
    pub id: String,

    /// Event title/summary
    pub title: String,

    /// Event description/body
    pub description: Option<String>,

    /// Event location
    pub location: Option<String>,

    /// Start time (Unix timestamp in seconds)
    pub start_timestamp: i64,

    /// End time (Unix timestamp in seconds)
    pub end_timestamp: i64,

    /// Whether this is an all-day event
    pub all_day: bool,

    /// Calendar name this event belongs to
    pub calendar_name: String,

    /// Organizer email/name
    pub organizer: Option<String>,

    /// List of reminder times (Unix timestamps in seconds)
    pub reminders: Vec<i64>,

    /// Event color (ARGB format)
    pub color: i32,

    /// Recurrence rule (iCalendar RRULE format)
    pub rrule: Option<String>,
}

impl CalendarEvent {
    /// Get the duration of the event in seconds
    pub fn duration_seconds(&self) -> i64 {
        self.end_timestamp - self.start_timestamp
    }

    /// Check if the event is currently happening
    pub fn is_active(&self) -> bool {
        let now = Utc::now().timestamp();
        self.start_timestamp <= now && now <= self.end_timestamp
    }

    /// Check if the event is in the future
    pub fn is_future(&self) -> bool {
        let now = Utc::now().timestamp();
        self.start_timestamp > now
    }
}

/// Calendar provider trait for different calendar backends
#[async_trait::async_trait]
pub trait CalendarProvider: Send + Sync {
    /// Get the name of this calendar provider
    fn name(&self) -> &str;

    /// Check if this provider is available on the system
    async fn is_available(&self) -> bool;

    /// Fetch calendar events within a time range
    ///
    /// # Arguments
    /// * `start` - Start of the time range (Unix timestamp in seconds)
    /// * `end` - End of the time range (Unix timestamp in seconds)
    /// * `max_events` - Maximum number of events to return (0 = no limit)
    /// * `include_all_day` - Whether to include all-day events
    async fn fetch_events(
        &self,
        start: i64,
        end: i64,
        max_events: usize,
        include_all_day: bool,
    ) -> Result<Vec<CalendarEvent>, CalendarError>;
}

/// Errors that can occur during calendar operations
#[derive(Debug, thiserror::Error)]
pub enum CalendarError {
    #[error("D-Bus error: {0}")]
    DBusError(#[from] zbus::Error),

    #[error("Calendar provider not available: {0}")]
    ProviderNotAvailable(String),

    #[error("Failed to parse calendar data: {0}")]
    ParseError(String),

    #[error("Invalid time range: start={0}, end={1}")]
    InvalidTimeRange(i64, i64),

    #[error("No calendar providers available")]
    NoProvidersAvailable,
}

/// URL-based calendar provider that downloads ICS files from HTTP/HTTPS endpoints
pub struct UrlCalendarProvider {
    urls: Vec<String>,
    client: reqwest::Client,
    cache: std::sync::Arc<tokio::sync::Mutex<HashMap<String, (String, std::time::Instant)>>>,
    cache_duration: std::time::Duration,
}

impl UrlCalendarProvider {
    /// Create a new URL calendar provider
    ///
    /// # Arguments
    /// * `urls` - List of URLs to fetch ICS files from
    /// * `cache_duration_secs` - How long to cache downloaded ICS data (0 = no cache)
    pub fn new(urls: Vec<String>, cache_duration_secs: u64) -> Result<Self, CalendarError> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| {
                CalendarError::ParseError(format!("Failed to create HTTP client: {}", e))
            })?;

        Ok(Self {
            urls,
            client,
            cache: std::sync::Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            cache_duration: std::time::Duration::from_secs(cache_duration_secs),
        })
    }

    /// Parse iCalendar file containing multiple VEVENT components
    fn parse_ical_file(
        ical_data: &str,
        calendar_name: String,
        color: i32,
    ) -> Result<Vec<CalendarEvent>, CalendarError> {
        let mut events = Vec::new();
        let mut vevent_count = 0;
        let mut failed_parse_count = 0;

        // Simple iCalendar parser for VEVENT components
        let mut in_calendar = false;
        let mut in_event = false;
        let mut current_event: HashMap<String, String> = HashMap::new();
        let mut multiline_key: Option<String> = None;
        let mut multiline_value = String::new();

        debug!(
            "ICS Parser: Starting to parse ICS data ({} bytes)",
            ical_data.len()
        );

        for line in ical_data.lines() {
            let line_trimmed = line.trim();

            // Handle line continuation (starts with space or tab in ORIGINAL line, not trimmed)
            if line.starts_with(' ') || line.starts_with('\t') {
                if multiline_key.is_some() {
                    multiline_value.push_str(line_trimmed);
                    continue;
                }
            } else if let Some(key) = multiline_key.take() {
                // Finish the multiline value
                debug!("ICS Parser: {} = {}", key, multiline_value);
                current_event.insert(key, multiline_value.clone());
                multiline_value.clear();
            }

            if line_trimmed == "BEGIN:VCALENDAR" {
                in_calendar = true;
            } else if line_trimmed == "END:VCALENDAR" {
                in_calendar = false;
            } else if line_trimmed == "BEGIN:VEVENT" && in_calendar {
                in_event = true;
                current_event.clear();
                vevent_count += 1;
                debug!("ICS Parser: Starting VEVENT #{}", vevent_count);
            } else if line_trimmed == "END:VEVENT" && in_event {
                // Flush any pending multiline value
                if let Some(key) = multiline_key.take() {
                    debug!("ICS Parser: {} = {}", key, multiline_value);
                    current_event.insert(key, multiline_value.clone());
                    multiline_value.clear();
                }

                debug!(
                    "ICS Parser: Event #{} properties: {:?}",
                    vevent_count,
                    current_event.keys().collect::<Vec<_>>()
                );
                if let Some(event) =
                    Self::build_event_from_ical(&current_event, calendar_name.clone(), color)
                {
                    debug!(
                        "ICS Parser: ✅ Successfully parsed event #{}: {}",
                        vevent_count, event.title
                    );
                    events.push(event);
                } else {
                    failed_parse_count += 1;
                    warn!(
                        "ICS Parser: ❌ Failed to build event #{} from properties. Missing: UID={}, SUMMARY={}, DTSTART={}",
                        vevent_count,
                        current_event.contains_key("UID"),
                        current_event.contains_key("SUMMARY"),
                        current_event.contains_key("DTSTART")
                    );
                    debug!("ICS Parser: Failed event properties: {:?}", current_event);
                }
                in_event = false;
            } else if in_event && !line_trimmed.is_empty() {
                if let Some((key, value)) = line_trimmed.split_once(':') {
                    // Handle property parameters (e.g., DTSTART;TZID=America/New_York:20240101T120000)
                    let key_parts: Vec<&str> = key.split(';').collect();
                    let prop_name = key_parts[0].to_string();

                    // Check if this might be a multiline value
                    multiline_key = Some(prop_name.clone());
                    multiline_value = value.to_string();
                }
            }
        }

        // Handle any remaining multiline value
        if let Some(key) = multiline_key {
            debug!("ICS Parser: {} = {}", key, multiline_value);
            current_event.insert(key, multiline_value);
        }

        info!(
            "ICS Parser: Found {} VEVENT blocks, successfully parsed {} events, failed to parse {} events",
            vevent_count, events.len(), failed_parse_count
        );
        if failed_parse_count > 0 {
            warn!(
                "ICS Parser: {} events failed to parse - check logs for missing required fields (UID, SUMMARY, DTSTART)",
                failed_parse_count
            );
        }
        Ok(events)
    }

    /// Expand a recurring event into individual instances within a time range
    fn expand_recurring_event(
        event: &CalendarEvent,
        start_range: i64,
        end_range: i64,
    ) -> Vec<CalendarEvent> {
        let mut expanded = Vec::new();

        // If no recurrence rule, return the original event
        let rrule = match &event.rrule {
            Some(r) => r,
            None => {
                expanded.push(event.clone());
                return expanded;
            }
        };

        debug!(
            "Expanding recurring event '{}' with RRULE: {}",
            event.title, rrule
        );

        // Parse RRULE - simple implementation for common cases
        let mut freq = None;
        let mut count = None;
        let mut until = None;
        let mut interval = 1;

        for part in rrule.split(';') {
            let part = part.trim();
            if part.starts_with("FREQ=") {
                freq = Some(&part[5..]);
            } else if part.starts_with("COUNT=") {
                count = part[6..].parse::<usize>().ok();
            } else if part.starts_with("UNTIL=") {
                until = Self::parse_ical_datetime(&part[6..]);
            } else if part.starts_with("INTERVAL=") {
                interval = part[9..].parse::<i64>().unwrap_or(1);
            }
        }

        let freq = match freq {
            Some(f) => f,
            None => {
                warn!("RRULE missing FREQ, returning original event");
                expanded.push(event.clone());
                return expanded;
            }
        };

        // Determine recurrence interval in seconds
        let recurrence_seconds = match freq {
            "DAILY" => 86400 * interval,
            "WEEKLY" => 604800 * interval,
            "MONTHLY" => 2592000 * interval, // Approximate (30 days)
            "YEARLY" => 31536000 * interval, // Approximate (365 days)
            _ => {
                warn!("Unsupported FREQ: {}, returning original event", freq);
                expanded.push(event.clone());
                return expanded;
            }
        };

        // Generate instances
        let mut current_start = event.start_timestamp;
        let duration = event.end_timestamp - event.start_timestamp;
        let mut instance_count = 0;
        let max_instances = count.unwrap_or(100); // Limit to prevent infinite loops

        while instance_count < max_instances {
            // Check if we're past the time range or UNTIL date
            if current_start > end_range {
                break;
            }
            if let Some(until_ts) = until {
                if current_start > until_ts {
                    break;
                }
            }

            // Only include instances that overlap with the requested range
            let current_end = current_start + duration;
            if current_end >= start_range && current_start <= end_range {
                let mut instance = event.clone();
                instance.start_timestamp = current_start;
                instance.end_timestamp = current_end;
                instance.rrule = None; // Remove RRULE from instances

                debug!(
                    "Generated instance {} of '{}' at timestamp {}",
                    instance_count + 1,
                    event.title,
                    current_start
                );

                expanded.push(instance);
            }

            // Move to next occurrence
            if freq == "MONTHLY" {
                // More accurate monthly recurrence
                let dt = match Utc.timestamp_opt(current_start, 0).single() {
                    Some(d) => d,
                    None => break,
                };
                let next = if interval == 1 {
                    // Add one month
                    let mut new_month = dt.month() + 1;
                    let mut new_year = dt.year();
                    if new_month > 12 {
                        new_month = 1;
                        new_year += 1;
                    }
                    match Utc
                        .with_ymd_and_hms(
                            new_year,
                            new_month,
                            dt.day().min(28), // Avoid invalid dates
                            dt.hour(),
                            dt.minute(),
                            dt.second(),
                        )
                        .single()
                    {
                        Some(d) => d.timestamp(),
                        None => current_start + recurrence_seconds,
                    }
                } else {
                    current_start + recurrence_seconds
                };
                current_start = next;
            } else if freq == "YEARLY" {
                // More accurate yearly recurrence
                let dt = match Utc.timestamp_opt(current_start, 0).single() {
                    Some(d) => d,
                    None => break,
                };
                let next = match Utc
                    .with_ymd_and_hms(
                        dt.year() + interval as i32,
                        dt.month(),
                        dt.day(),
                        dt.hour(),
                        dt.minute(),
                        dt.second(),
                    )
                    .single()
                {
                    Some(d) => d.timestamp(),
                    None => current_start + recurrence_seconds,
                };
                current_start = next;
            } else {
                current_start += recurrence_seconds;
            }

            instance_count += 1;
        }

        info!(
            "Expanded recurring event '{}' into {} instances",
            event.title,
            expanded.len()
        );
        expanded
    }

    /// Build a CalendarEvent from parsed iCalendar properties
    fn build_event_from_ical(
        props: &HashMap<String, String>,
        calendar_name: String,
        color: i32,
    ) -> Option<CalendarEvent> {
        let uid = props.get("UID")?.clone();
        let title = props.get("SUMMARY")?.clone();

        // Parse start time
        let start_str = props.get("DTSTART")?;
        let start_timestamp = Self::parse_ical_datetime(start_str)?;

        // Parse end time (or use DURATION)
        let end_timestamp = if let Some(end_str) = props.get("DTEND") {
            Self::parse_ical_datetime(end_str)?
        } else if let Some(duration_str) = props.get("DURATION") {
            start_timestamp + Self::parse_ical_duration(duration_str)?
        } else {
            // Default to 1 hour if no end or duration
            start_timestamp + 3600
        };

        // Check if it's an all-day event (dates without time component)
        let all_day = start_str.len() == 8; // YYYYMMDD format

        Some(CalendarEvent {
            id: uid,
            title,
            description: props.get("DESCRIPTION").cloned(),
            location: props.get("LOCATION").cloned(),
            start_timestamp,
            end_timestamp,
            all_day,
            calendar_name,
            organizer: props.get("ORGANIZER").cloned(),
            reminders: Self::parse_ical_alarms(props),
            color,
            rrule: props.get("RRULE").cloned(),
        })
    }

    /// Parse iCalendar date-time string to Unix timestamp
    fn parse_ical_datetime(dt_str: &str) -> Option<i64> {
        // Handle different formats:
        // - YYYYMMDD (date only, all-day event)
        // - YYYYMMDDTHHMMSS (local time)
        // - YYYYMMDDTHHMMSSZ (UTC time)
        // - VALUE=DATE:YYYYMMDD (with parameter)

        // Strip VALUE=DATE: prefix if present
        let clean_str = if dt_str.contains("VALUE=DATE:") {
            dt_str.split("VALUE=DATE:").last().unwrap_or(dt_str).trim()
        } else if dt_str.contains(':') && !dt_str.contains('T') {
            // Handle other parameter formats like DTSTART;VALUE=DATE:20240101
            dt_str.split(':').last().unwrap_or(dt_str).trim()
        } else {
            dt_str.trim()
        };

        debug!(
            "ICS Parser: Parsing datetime '{}' (original: '{}')",
            clean_str, dt_str
        );

        if clean_str.len() == 8 {
            // Date only (all-day event) - use midnight UTC
            let year = match clean_str[0..4].parse::<i32>() {
                Ok(y) => y,
                Err(e) => {
                    warn!(
                        "ICS Parser: Failed to parse year from '{}': {}",
                        clean_str, e
                    );
                    return None;
                }
            };
            let month = match clean_str[4..6].parse::<u32>() {
                Ok(m) => m,
                Err(e) => {
                    warn!(
                        "ICS Parser: Failed to parse month from '{}': {}",
                        clean_str, e
                    );
                    return None;
                }
            };
            let day = match clean_str[6..8].parse::<u32>() {
                Ok(d) => d,
                Err(e) => {
                    warn!(
                        "ICS Parser: Failed to parse day from '{}': {}",
                        clean_str, e
                    );
                    return None;
                }
            };

            match Utc.with_ymd_and_hms(year, month, day, 0, 0, 0).single() {
                Some(dt) => {
                    debug!(
                        "ICS Parser: Parsed date '{}' -> {}",
                        clean_str,
                        dt.timestamp()
                    );
                    return Some(dt.timestamp());
                }
                None => {
                    warn!("ICS Parser: Invalid date {}-{:02}-{:02}", year, month, day);
                    return None;
                }
            }
        }

        if clean_str.len() >= 15 {
            // Date-time format
            let year = match clean_str[0..4].parse::<i32>() {
                Ok(y) => y,
                Err(e) => {
                    warn!(
                        "ICS Parser: Failed to parse year from datetime '{}': {}",
                        clean_str, e
                    );
                    return None;
                }
            };
            let month = match clean_str[4..6].parse::<u32>() {
                Ok(m) => m,
                Err(e) => {
                    warn!(
                        "ICS Parser: Failed to parse month from datetime '{}': {}",
                        clean_str, e
                    );
                    return None;
                }
            };
            let day = match clean_str[6..8].parse::<u32>() {
                Ok(d) => d,
                Err(e) => {
                    warn!(
                        "ICS Parser: Failed to parse day from datetime '{}': {}",
                        clean_str, e
                    );
                    return None;
                }
            };
            let hour = match clean_str[9..11].parse::<u32>() {
                Ok(h) => h,
                Err(e) => {
                    warn!(
                        "ICS Parser: Failed to parse hour from datetime '{}': {}",
                        clean_str, e
                    );
                    return None;
                }
            };
            let minute = match clean_str[11..13].parse::<u32>() {
                Ok(m) => m,
                Err(e) => {
                    warn!(
                        "ICS Parser: Failed to parse minute from datetime '{}': {}",
                        clean_str, e
                    );
                    return None;
                }
            };
            let second = match clean_str[13..15].parse::<u32>() {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        "ICS Parser: Failed to parse second from datetime '{}': {}",
                        clean_str, e
                    );
                    return None;
                }
            };

            // Check if this is UTC (ends with Z) or local time
            let timestamp = if clean_str.ends_with('Z') {
                // UTC time - parse as UTC
                match Utc
                    .with_ymd_and_hms(year, month, day, hour, minute, second)
                    .single()
                {
                    Some(dt) => {
                        debug!(
                            "ICS Parser: Parsed UTC datetime '{}' -> {}",
                            clean_str,
                            dt.timestamp()
                        );
                        dt.timestamp()
                    }
                    None => {
                        warn!(
                            "ICS Parser: Invalid UTC datetime {}-{:02}-{:02} {:02}:{:02}:{:02}",
                            year, month, day, hour, minute, second
                        );
                        return None;
                    }
                }
            } else {
                // Local time - parse as local timezone then convert to UTC timestamp
                match Local
                    .with_ymd_and_hms(year, month, day, hour, minute, second)
                    .single()
                {
                    Some(dt) => {
                        let utc_timestamp = dt.timestamp();
                        debug!(
                            "ICS Parser: Parsed local datetime '{}' -> {} (UTC timestamp)",
                            clean_str, utc_timestamp
                        );
                        utc_timestamp
                    }
                    None => {
                        warn!(
                            "ICS Parser: Invalid local datetime {}-{:02}-{:02} {:02}:{:02}:{:02}",
                            year, month, day, hour, minute, second
                        );
                        return None;
                    }
                }
            };

            return Some(timestamp);
        }

        warn!(
            "ICS Parser: Unrecognized datetime format: '{}' (length={})",
            clean_str,
            clean_str.len()
        );
        None
    }

    /// Parse iCalendar DURATION string to seconds
    fn parse_ical_duration(duration_str: &str) -> Option<i64> {
        // Simple duration parser for formats like:
        // P7D (7 days)
        // PT1H (1 hour)
        // PT30M (30 minutes)
        // P1DT2H30M (1 day, 2 hours, 30 minutes)

        let mut seconds: i64 = 0;
        let duration = duration_str.trim();

        if !duration.starts_with('P') {
            return None;
        }

        let duration = &duration[1..]; // Remove 'P'
        let parts: Vec<&str> = duration.split('T').collect();

        // Parse date part
        if !parts.is_empty() {
            let date_part = parts[0];
            if let Some(pos) = date_part.find('D') {
                let days: i64 = date_part[..pos].parse().ok()?;
                seconds += days * 86400;
            }
        }

        // Parse time part
        if parts.len() > 1 {
            let time_part = parts[1];
            let mut remaining = time_part;

            if let Some(pos) = remaining.find('H') {
                let hours: i64 = remaining[..pos].parse().ok()?;
                seconds += hours * 3600;
                remaining = &remaining[pos + 1..];
            }

            if let Some(pos) = remaining.find('M') {
                let minutes: i64 = remaining[..pos].parse().ok()?;
                seconds += minutes * 60;
                remaining = &remaining[pos + 1..];
            }

            if let Some(pos) = remaining.find('S') {
                let secs: i64 = remaining[..pos].parse().ok()?;
                seconds += secs;
            }
        }

        Some(seconds)
    }

    /// Parse VALARM components from iCalendar properties
    fn parse_ical_alarms(_props: &HashMap<String, String>) -> Vec<i64> {
        // Simplified implementation - full version would parse VALARM components
        Vec::new()
    }

    /// Create from environment variable CALENDAR_URLS (comma-separated)
    pub fn from_env() -> Option<Self> {
        let urls_str = std::env::var("CALENDAR_URLS").ok()?;
        let urls: Vec<String> = urls_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if urls.is_empty() {
            return None;
        }

        let cache_duration = std::env::var("CALENDAR_CACHE_DURATION")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300); // Default 5 minutes

        Self::new(urls, cache_duration).ok()
    }

    /// Download ICS data from a URL with caching
    async fn fetch_ics_from_url(&self, url: &str) -> Result<String, CalendarError> {
        // Check cache first
        {
            let cache = self.cache.lock().await;
            if let Some((data, timestamp)) = cache.get(url) {
                if timestamp.elapsed() < self.cache_duration {
                    debug!("Using cached ICS data for URL: {}", url);
                    return Ok(data.clone());
                }
            }
        }

        info!("Downloading ICS from URL: {}", url);

        // Download the ICS file
        let response = self.client.get(url).send().await.map_err(|e| {
            CalendarError::ParseError(format!("Failed to download from {}: {}", url, e))
        })?;

        if !response.status().is_success() {
            return Err(CalendarError::ParseError(format!(
                "HTTP error {} downloading from {}",
                response.status(),
                url
            )));
        }

        let ics_data = response.text().await.map_err(|e| {
            CalendarError::ParseError(format!("Failed to read response from {}: {}", url, e))
        })?;

        // Update cache
        {
            let mut cache = self.cache.lock().await;
            cache.insert(
                url.to_string(),
                (ics_data.clone(), std::time::Instant::now()),
            );
        }

        info!("Downloaded {} bytes from {}", ics_data.len(), url);
        Ok(ics_data)
    }
}

#[async_trait::async_trait]
impl CalendarProvider for UrlCalendarProvider {
    fn name(&self) -> &str {
        "URL Calendar Provider"
    }

    async fn is_available(&self) -> bool {
        !self.urls.is_empty()
    }

    async fn fetch_events(
        &self,
        start: i64,
        end: i64,
        max_events: usize,
        include_all_day: bool,
    ) -> Result<Vec<CalendarEvent>, CalendarError> {
        if start >= end {
            return Err(CalendarError::InvalidTimeRange(start, end));
        }

        info!("Fetching calendar events from {} URL(s)", self.urls.len());

        let mut all_events = Vec::new();

        for url in &self.urls {
            match self.fetch_ics_from_url(url).await {
                Ok(ics_data) => {
                    match UrlCalendarProvider::parse_ical_file(&ics_data, url.clone(), 0) {
                        Ok(events) => {
                            // Expand recurring events
                            let mut expanded_events = Vec::new();
                            for event in events {
                                if event.rrule.is_some() {
                                    let instances = UrlCalendarProvider::expand_recurring_event(
                                        &event, start, end,
                                    );
                                    expanded_events.extend(instances);
                                } else {
                                    expanded_events.push(event);
                                }
                            }

                            // Filter by date range and all_day preference
                            expanded_events.retain(|event| {
                                let in_range =
                                    event.end_timestamp >= start && event.start_timestamp <= end;
                                let day_filter = if include_all_day {
                                    true
                                } else {
                                    !event.all_day
                                };
                                in_range && day_filter
                            });

                            info!("Fetched {} events from {}", expanded_events.len(), url);
                            all_events.extend(expanded_events);
                        }
                        Err(e) => {
                            error!("Failed to parse ICS from {}: {}", url, e);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to download from {}: {}", url, e);
                }
            }
        }

        // Sort by start time
        all_events.sort_by_key(|e| e.start_timestamp);

        // Limit to max_events
        if max_events > 0 && all_events.len() > max_events {
            all_events.truncate(max_events);
        }

        info!("Total events fetched from URLs: {}", all_events.len());
        Ok(all_events)
    }
}

/// Calendar manager that aggregates multiple calendar providers
pub struct CalendarManager {
    providers: Vec<Box<dyn CalendarProvider>>,
}

impl CalendarManager {
    /// Create a new calendar manager with default providers
    pub async fn new() -> Result<Self, CalendarError> {
        let mut providers: Vec<Box<dyn CalendarProvider>> = Vec::new();

        // Try to add URL calendar provider from environment
        if let Some(provider) = UrlCalendarProvider::from_env() {
            if provider.is_available().await {
                info!(
                    "URL Calendar provider is available with {} URL(s)",
                    provider.urls.len()
                );
                providers.push(Box::new(provider));
            }
        }

        if providers.is_empty() {
            return Err(CalendarError::NoProvidersAvailable);
        }

        Ok(Self { providers })
    }

    /// Create a calendar manager with custom providers
    pub fn with_providers(
        providers: Vec<Box<dyn CalendarProvider>>,
    ) -> Result<Self, CalendarError> {
        if providers.is_empty() {
            return Err(CalendarError::NoProvidersAvailable);
        }
        Ok(Self { providers })
    }

    /// Add a URL calendar provider
    pub fn add_url_provider(
        &mut self,
        urls: Vec<String>,
        cache_duration_secs: u64,
    ) -> Result<(), CalendarError> {
        let provider = UrlCalendarProvider::new(urls, cache_duration_secs)?;
        self.providers.push(Box::new(provider));
        Ok(())
    }

    /// Fetch events from all available providers
    pub async fn fetch_events(
        &self,
        start: i64,
        end: i64,
        max_events: usize,
        include_all_day: bool,
    ) -> Result<Vec<CalendarEvent>, CalendarError> {
        let mut all_events = Vec::new();

        for provider in &self.providers {
            match provider
                .fetch_events(start, end, max_events, include_all_day)
                .await
            {
                Ok(mut events) => {
                    info!("Fetched {} events from {}", events.len(), provider.name());
                    all_events.append(&mut events);
                }
                Err(e) => {
                    error!("Failed to fetch events from {}: {}", provider.name(), e);
                }
            }
        }

        // Sort events by start time
        all_events.sort_by_key(|e| e.start_timestamp);

        // Limit to max_events if specified
        if max_events > 0 && all_events.len() > max_events {
            all_events.truncate(max_events);
        }

        Ok(all_events)
    }

    /// Get the number of available providers
    pub fn provider_count(&self) -> usize {
        self.providers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ical_datetime_date_only() {
        let result = UrlCalendarProvider::parse_ical_datetime("20240315");
        assert!(result.is_some());
        let timestamp = result.unwrap();
        let dt = Utc.timestamp_opt(timestamp, 0).unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 3);
        assert_eq!(dt.day(), 15);
    }

    #[test]
    fn test_parse_ical_datetime_with_time() {
        let result = UrlCalendarProvider::parse_ical_datetime("20240315T143000Z");
        assert!(result.is_some());
        let timestamp = result.unwrap();
        let dt = Utc.timestamp_opt(timestamp, 0).unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 3);
        assert_eq!(dt.day(), 15);
        assert_eq!(dt.hour(), 14);
        assert_eq!(dt.minute(), 30);
        assert_eq!(dt.second(), 0);
    }

    #[test]
    fn test_parse_ical_duration() {
        assert_eq!(UrlCalendarProvider::parse_ical_duration("PT1H"), Some(3600));
        assert_eq!(
            UrlCalendarProvider::parse_ical_duration("PT30M"),
            Some(1800)
        );
        assert_eq!(
            UrlCalendarProvider::parse_ical_duration("P7D"),
            Some(604800)
        );
        assert_eq!(
            UrlCalendarProvider::parse_ical_duration("P1DT2H30M"),
            Some(95400)
        );
    }

    #[test]
    fn test_calendar_event_duration() {
        let event = CalendarEvent {
            id: "test-1".to_string(),
            title: "Test Event".to_string(),
            description: None,
            location: None,
            start_timestamp: 1000,
            end_timestamp: 2000,
            all_day: false,
            calendar_name: "Test Calendar".to_string(),
            organizer: None,
            reminders: vec![],
            color: 0,
            rrule: None,
        };

        assert_eq!(event.duration_seconds(), 1000);
    }
}
