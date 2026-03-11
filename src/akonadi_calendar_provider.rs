// SPDX-License-Identifier: AGPL-3.0
//
// src/akonadi_calendar_provider.rs
//
// Implements `CalendarProvider` on top of the local KDE Akonadi PIM store.
//
// # Architecture
//
// Qt (and therefore Akonadi) requires that every Qt object lives on — and is
// only accessed from — the OS thread that created it.  Tokio's async executor
// does not make thread-affinity guarantees, so we cannot call the C++ bridge
// directly from async tasks.
//
// Instead we spin up a single, long-lived **Qt thread** when the provider is
// first constructed.  That thread:
//   1. calls `ffi::akonadi_qt_init()` to create a `QCoreApplication`, and
//   2. loops forever, receiving `QtRequest` values from a `SyncSender` and
//      executing each one synchronously (Akonadi's `KJob::exec()` drives its
//      own nested `QEventLoop`).
//
// The async `fetch_events` method sends a request to the Qt thread and awaits
// a `tokio::sync::oneshot` response, bridging the two worlds cleanly.
//
// # ICS parsing
//
// The C++ layer returns raw ICS payload strings (one per Akonadi item).
// We reuse `UrlCalendarProvider`'s `pub(crate)` parsing helpers —
// `parse_ical_file` and `expand_recurring_event` — so that the two providers
// stay in sync with respect to iCalendar semantics.

use std::sync::OnceLock;
use std::thread;

use async_trait::async_trait;
use log::{debug, error, info, warn};
use tokio::sync::oneshot;

use crate::akonadi::ffi;
use crate::calendar::{CalendarError, CalendarEvent, CalendarProvider, UrlCalendarProvider};

// ---------------------------------------------------------------------------
// Qt-thread request / response types
// ---------------------------------------------------------------------------

/// A single request sent to the dedicated Qt OS thread.
enum QtRequest {
    /// Check whether the Akonadi server is reachable.
    IsAvailable {
        reply: oneshot::Sender<bool>,
    },
    /// Fetch all calendar ICS payloads from Akonadi.
    FetchIcs {
        start_ts: i64,
        end_ts: i64,
        reply: oneshot::Sender<Result<Vec<String>, String>>,
    },
}

// ---------------------------------------------------------------------------
// AkonadiHandle — owns the Qt thread and the channel to talk to it
// ---------------------------------------------------------------------------

/// A cloneable handle to the single, long-lived Qt OS thread.
///
/// The first call to `AkonadiHandle::get()` spawns the thread and stores it
/// in a process-global `OnceLock`; subsequent calls return a clone of the
/// same sender, which is cheap (`SyncSender` is just an `Arc` internally).
#[derive(Clone)]
pub struct AkonadiHandle {
    /// Sender half of the channel to the Qt thread.
    /// `SyncSender` with `bound = 0` makes each send block until the Qt
    /// thread is ready to accept — this provides natural back-pressure and
    /// prevents the queue from growing unboundedly.
    tx: std::sync::mpsc::SyncSender<QtRequest>,
}

/// Process-global handle, initialised at most once.
static AKONADI_HANDLE: OnceLock<AkonadiHandle> = OnceLock::new();

impl AkonadiHandle {
    /// Return the global handle, spawning the Qt thread if this is the first call.
    pub fn get() -> &'static Self {
        AKONADI_HANDLE.get_or_init(Self::spawn)
    }

    /// Spawn the dedicated Qt OS thread and return a handle to it.
    fn spawn() -> Self {
        // Bound of 1 lets us buffer one request while the previous one is
        // still running, avoiding a deadlock if the caller sends just before
        // the Qt thread loops back to `recv()`.
        let (tx, rx) = std::sync::mpsc::sync_channel::<QtRequest>(1);

        thread::Builder::new()
            .name("watchd-qt-akonadi".to_string())
            .spawn(move || {
                // -------------------------------------------------------
                // Initialise Qt on this thread — must happen before any
                // Akonadi API call.
                // -------------------------------------------------------
                info!("[akonadi] Qt thread starting — calling akonadi_qt_init()");
                // SAFETY: we are the only code that touches Qt objects; this
                // thread is the designated Qt thread for the process.
                ffi::akonadi_qt_init();
                info!("[akonadi] Qt thread ready.");

                // -------------------------------------------------------
                // Main loop — process requests until the sender is dropped.
                // -------------------------------------------------------
                while let Ok(request) = rx.recv() {
                    match request {
                        QtRequest::IsAvailable { reply } => {
                            // SAFETY: called from the Qt thread.
                            let available = ffi::akonadi_is_available();
                            // Ignore send errors — the requester may have
                            // timed out or been dropped.
                            let _ = reply.send(available);
                        }

                        QtRequest::FetchIcs {
                            start_ts,
                            end_ts,
                            reply,
                        } => {
                            debug!(
                                "[akonadi] Fetching ICS payloads \
                                 (start={}, end={})…",
                                start_ts, end_ts
                            );
                            // SAFETY: called from the Qt thread.
                            let ics_vec: Vec<String> =
                                ffi::akonadi_fetch_calendar_ics(start_ts, end_ts)
                                    .into_iter()
                                    .map(|s| s.to_string())
                                    .collect();

                            debug!(
                                "[akonadi] Received {} ICS payload(s) from C++ layer.",
                                ics_vec.len()
                            );
                            let _ = reply.send(Ok(ics_vec));
                        }
                    }
                }

                info!("[akonadi] Qt thread shutting down (channel closed).");
            })
            .expect("failed to spawn watchd-qt-akonadi thread");

        Self { tx }
    }

    /// Ask the Qt thread whether Akonadi is currently available.
    ///
    /// Returns `false` on any channel or timeout error so that the provider
    /// degrades gracefully rather than panicking.
    async fn is_available(&self) -> bool {
        let (reply_tx, reply_rx) = oneshot::channel();

        if self
            .tx
            .send(QtRequest::IsAvailable { reply: reply_tx })
            .is_err()
        {
            error!("[akonadi] Failed to send IsAvailable request to Qt thread.");
            return false;
        }

        match reply_rx.await {
            Ok(available) => available,
            Err(_) => {
                error!("[akonadi] Qt thread dropped the IsAvailable reply channel.");
                false
            }
        }
    }

    /// Ask the Qt thread to fetch all calendar ICS payloads from Akonadi.
    async fn fetch_ics(
        &self,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<String>, CalendarError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.tx
            .send(QtRequest::FetchIcs {
                start_ts,
                end_ts,
                reply: reply_tx,
            })
            .map_err(|e| {
                CalendarError::ProviderNotAvailable(format!(
                    "Failed to send FetchIcs request to Qt thread: {}",
                    e
                ))
            })?;

        reply_rx
            .await
            .map_err(|_| {
                CalendarError::ProviderNotAvailable(
                    "Qt thread dropped the FetchIcs reply channel.".to_string(),
                )
            })?
            .map_err(|e| CalendarError::ParseError(e))
    }
}

// ---------------------------------------------------------------------------
// AkonadiCalendarProvider
// ---------------------------------------------------------------------------

/// A `CalendarProvider` that reads events from the local KDE Akonadi store.
///
/// Create one with [`AkonadiCalendarProvider::new()`].  Multiple instances
/// share the same underlying Qt thread via `AkonadiHandle::get()`.
///
/// # Availability
///
/// The provider reports itself as unavailable (and `fetch_events` returns an
/// error) when the Akonadi server daemon is not running.  This is a normal
/// condition on non-KDE desktops; `CalendarManager` will simply skip this
/// provider and continue with any others that are available.
pub struct AkonadiCalendarProvider {
    /// Human-readable name shown in logs.
    name: String,
    /// Handle to the single Qt OS thread.
    handle: AkonadiHandle,
}

impl AkonadiCalendarProvider {
    /// Create a new provider.
    ///
    /// This does **not** connect to Akonadi immediately; the connection is
    /// made lazily on the first `fetch_events` call (or when `is_available`
    /// is polled).  The underlying Qt thread is spawned the first time any
    /// `AkonadiCalendarProvider` is constructed.
    pub fn new() -> Self {
        info!("[akonadi] Constructing AkonadiCalendarProvider.");
        Self {
            name: "Akonadi Calendar Provider".to_string(),
            handle: AkonadiHandle::get().clone(),
        }
    }

    /// Parse a slice of raw ICS payload strings into `CalendarEvent` objects,
    /// expand recurring events, and apply the time-range / all-day filters.
    fn parse_and_filter(
        ics_payloads: Vec<String>,
        start: i64,
        end: i64,
        max_events: usize,
        include_all_day: bool,
    ) -> Vec<CalendarEvent> {
        let mut all_events: Vec<CalendarEvent> = Vec::new();

        for (idx, ics) in ics_payloads.iter().enumerate() {
            // Use the collection index as a rough display name; Akonadi does
            // not give us the collection name in the payload itself.
            let calendar_name = format!("Akonadi Calendar {}", idx + 1);

            match UrlCalendarProvider::parse_ical_file(ics, calendar_name.clone(), 0) {
                Ok(events) => {
                    for event in events {
                        if event.rrule.is_some() {
                            // Expand recurring events into individual instances
                            // that fall within [start, end].
                            let instances =
                                UrlCalendarProvider::expand_recurring_event(&event, start, end);
                            all_events.extend(instances);
                        } else {
                            all_events.push(event);
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "[akonadi] Failed to parse ICS payload #{}: {}",
                        idx + 1,
                        e
                    );
                }
            }
        }

        // Apply time-range filter.
        all_events.retain(|event| {
            let in_range = event.end_timestamp >= start && event.start_timestamp <= end;
            let day_ok = include_all_day || !event.all_day;
            in_range && day_ok
        });

        // Sort chronologically.
        all_events.sort_by_key(|e| e.start_timestamp);

        // Honour max_events limit.
        if max_events > 0 && all_events.len() > max_events {
            all_events.truncate(max_events);
        }

        all_events
    }
}

impl Default for AkonadiCalendarProvider {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// CalendarProvider impl
// ---------------------------------------------------------------------------

#[async_trait]
impl CalendarProvider for AkonadiCalendarProvider {
    fn name(&self) -> &str {
        &self.name
    }

    async fn is_available(&self) -> bool {
        self.handle.is_available().await
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

        info!(
            "[akonadi] fetch_events(start={}, end={}, max={}, all_day={})",
            start, end, max_events, include_all_day
        );

        // Check availability before attempting a (potentially slow) fetch.
        if !self.handle.is_available().await {
            return Err(CalendarError::ProviderNotAvailable(
                "Akonadi server is not running".to_string(),
            ));
        }

        let ics_payloads = self.handle.fetch_ics(start, end).await?;

        info!(
            "[akonadi] Received {} raw ICS payload(s); parsing…",
            ics_payloads.len()
        );

        let events =
            Self::parse_and_filter(ics_payloads, start, end, max_events, include_all_day);

        info!("[akonadi] Returning {} parsed CalendarEvent(s).", events.len());

        Ok(events)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that the parse_and_filter helper correctly handles an empty
    /// input and applies the max_events cap.
    #[test]
    fn test_parse_and_filter_empty() {
        let events = AkonadiCalendarProvider::parse_and_filter(
            vec![],
            0,
            i64::MAX,
            0,
            true,
        );
        assert!(events.is_empty());
    }

    /// Verify that events outside the requested time window are excluded.
    #[test]
    fn test_parse_and_filter_time_range() {
        // Build a minimal VCALENDAR with one event far in the past.
        let past_ics = "\
BEGIN:VCALENDAR\r\n\
VERSION:2.0\r\n\
BEGIN:VEVENT\r\n\
UID:past-event@test\r\n\
SUMMARY:Past Event\r\n\
DTSTART:19900101T120000Z\r\n\
DTEND:19900101T130000Z\r\n\
END:VEVENT\r\n\
END:VCALENDAR\r\n"
            .to_string();

        // Window is now → now+1d; the past event should be filtered out.
        let now = chrono::Utc::now().timestamp();
        let events = AkonadiCalendarProvider::parse_and_filter(
            vec![past_ics],
            now,
            now + 86400,
            0,
            true,
        );
        assert!(
            events.is_empty(),
            "Expected past event to be filtered out, got: {:?}",
            events
        );
    }

    /// Verify that the max_events cap is honoured.
    #[test]
    fn test_parse_and_filter_max_events() {
        let now = chrono::Utc::now();
        let make_event = |uid: &str, offset_hours: i64| {
            let dt = now + chrono::Duration::hours(offset_hours);
            let dtstr = dt.format("%Y%m%dT%H%M%SZ").to_string();
            let dtend = (dt + chrono::Duration::hours(1))
                .format("%Y%m%dT%H%M%SZ")
                .to_string();
            format!(
                "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:{uid}\r\n\
                 SUMMARY:Event {uid}\r\nDTSTART:{dtstr}\r\nDTEND:{dtend}\r\n\
                 END:VEVENT\r\nEND:VCALENDAR\r\n"
            )
        };

        let payloads = vec![
            make_event("e1", 1),
            make_event("e2", 2),
            make_event("e3", 3),
        ];

        let window_start = now.timestamp();
        let window_end = now.timestamp() + 86400 * 7;

        let events = AkonadiCalendarProvider::parse_and_filter(
            payloads,
            window_start,
            window_end,
            2, // max_events = 2
            true,
        );
        assert_eq!(
            events.len(),
            2,
            "Expected exactly 2 events (max_events cap), got {}",
            events.len()
        );
    }

    /// Verify that all-day events are excluded when include_all_day is false.
    #[test]
    fn test_parse_and_filter_all_day_exclusion() {
        let now = chrono::Utc::now();
        let today = now.format("%Y%m%d").to_string();
        let tomorrow = (now + chrono::Duration::days(1))
            .format("%Y%m%d")
            .to_string();

        let all_day_ics = format!(
            "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n\
             BEGIN:VEVENT\r\n\
             UID:all-day@test\r\n\
             SUMMARY:All Day Event\r\n\
             DTSTART;VALUE=DATE:{today}\r\n\
             DTEND;VALUE=DATE:{tomorrow}\r\n\
             END:VEVENT\r\n\
             END:VCALENDAR\r\n"
        );

        let window_end = now.timestamp() + 86400 * 7;

        // With include_all_day = false, the all-day event should not appear.
        let events = AkonadiCalendarProvider::parse_and_filter(
            vec![all_day_ics.clone()],
            now.timestamp() - 86400,
            window_end,
            0,
            false, // exclude all-day
        );
        assert!(
            events.is_empty(),
            "Expected all-day event to be excluded, got: {:?}",
            events
        );

        // With include_all_day = true, it should appear.
        let events_inclusive = AkonadiCalendarProvider::parse_and_filter(
            vec![all_day_ics],
            now.timestamp() - 86400,
            window_end,
            0,
            true, // include all-day
        );
        assert_eq!(
            events_inclusive.len(),
            1,
            "Expected 1 all-day event when include_all_day=true, got {}",
            events_inclusive.len()
        );
    }
}
