//! Watchdog module for monitoring connection health and managing automatic reconnection
//!
//! This module provides a robust watchdog mechanism that:
//! - Monitors BLE connection state
//! - Tracks message traffic (RX timestamps)
//! - Detects connection stalls (no traffic within timeout)
//! - Manages automatic reconnection with exponential backoff
//! - Provides health metrics and diagnostics

use bluer::Device;
use log::debug;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;

/// Reasons why a reconnection was triggered
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectReason {
    /// BLE connection lost (device disconnected)
    ConnectionLost,
    /// No RX traffic within timeout period
    RxTimeout,
    /// Manual reconnection requested
    Manual,
    /// Initial connection attempt
    Initial,
}

impl std::fmt::Display for ReconnectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReconnectReason::ConnectionLost => write!(f, "Connection Lost"),
            ReconnectReason::RxTimeout => write!(f, "RX Timeout"),
            ReconnectReason::Manual => write!(f, "Manual"),
            ReconnectReason::Initial => write!(f, "Initial"),
        }
    }
}

/// Configuration for the watchdog
#[derive(Debug, Clone)]
pub struct WatchdogConfig {
    /// Maximum time without receiving any messages before triggering reconnection
    pub rx_timeout: Duration,

    /// How often to check connection health
    pub check_interval: Duration,

    /// Initial backoff delay after first reconnection attempt
    pub initial_backoff: Duration,

    /// Maximum backoff delay
    pub max_backoff: Duration,

    /// Backoff multiplier for exponential backoff
    pub backoff_multiplier: f64,

    /// Maximum number of consecutive reconnection attempts before giving up
    pub max_reconnect_attempts: u32,

    /// How long to wait after successful connection before resetting attempt counter
    pub stable_connection_threshold: Duration,
}

impl Default for WatchdogConfig {
    fn default() -> Self {
        Self {
            rx_timeout: Duration::from_secs(120), // 2 minutes - not too aggressive
            check_interval: Duration::from_secs(30), // Check every 30s
            initial_backoff: Duration::from_secs(5), // Start with 5s backoff
            max_backoff: Duration::from_secs(30), // Max 30s backoff
            backoff_multiplier: 2.0,
            max_reconnect_attempts: 5, // Only try 5 times
            stable_connection_threshold: Duration::from_secs(300), // 5 minutes
        }
    }
}

/// Health status of the connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// Connection is healthy
    Healthy,
    /// Connection is degraded (approaching timeout)
    Degraded,
    /// Connection is unhealthy (needs reconnection)
    Unhealthy,
    /// Currently reconnecting
    Reconnecting,
    /// Disconnected
    Disconnected,
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "Healthy"),
            HealthStatus::Degraded => write!(f, "Degraded"),
            HealthStatus::Unhealthy => write!(f, "Unhealthy"),
            HealthStatus::Reconnecting => write!(f, "Reconnecting"),
            HealthStatus::Disconnected => write!(f, "Disconnected"),
        }
    }
}

/// Internal state of the watchdog
struct WatchdogState {
    /// Last time a message was received
    last_rx: Option<Instant>,

    /// Time when connection was established
    connection_start: Option<Instant>,

    /// Current connection attempt number (resets on stable connection)
    reconnect_attempts: u32,

    /// Current backoff delay
    current_backoff: Duration,

    /// Total number of reconnections since start
    total_reconnects: u64,

    /// Last reconnection reason
    last_reconnect_reason: Option<ReconnectReason>,

    /// Time of last reconnection attempt
    last_reconnect_time: Option<Instant>,

    /// Current health status
    health_status: HealthStatus,

    /// Whether reconnection is currently in progress
    is_reconnecting: bool,
}

impl WatchdogState {
    fn new() -> Self {
        Self {
            last_rx: None,
            connection_start: None,
            reconnect_attempts: 0,
            current_backoff: Duration::from_secs(1),
            total_reconnects: 0,
            last_reconnect_reason: None,
            last_reconnect_time: None,
            health_status: HealthStatus::Disconnected,
            is_reconnecting: false,
        }
    }
}

/// Watchdog manager for connection health monitoring
pub struct WatchdogManager {
    config: WatchdogConfig,
    state: Arc<Mutex<WatchdogState>>,
}

impl WatchdogManager {
    /// Create a new watchdog manager with default configuration
    pub fn new() -> Self {
        Self::with_config(WatchdogConfig::default())
    }

    /// Create a new watchdog manager with custom configuration
    pub fn with_config(config: WatchdogConfig) -> Self {
        let state = WatchdogState::new();
        Self {
            config,
            state: Arc::new(Mutex::new(state)),
        }
    }

    /// Record that a message was received
    pub async fn record_rx(&self) {
        let mut state = self.state.lock().await;
        let now = Instant::now();
        state.last_rx = Some(now);
        debug!("🟢 Watchdog: RX traffic recorded at {:?}", now);

        // Update health status if we were degraded
        if state.health_status != HealthStatus::Healthy && !state.is_reconnecting {
            state.health_status = HealthStatus::Healthy;
        }
    }

    /// Mark that connection was established
    pub async fn mark_connected(&self) {
        let mut state = self.state.lock().await;
        let now = Instant::now();
        state.connection_start = Some(now);
        state.last_rx = Some(now);
        state.health_status = HealthStatus::Healthy;
        state.is_reconnecting = false;

        // Check if connection was stable long enough to reset attempt counter
        if let Some(last_reconnect) = state.last_reconnect_time {
            if now.duration_since(last_reconnect) >= self.config.stable_connection_threshold {
                state.reconnect_attempts = 0;
                state.current_backoff = self.config.initial_backoff;
            }
        }
    }

    /// Mark that connection was lost
    pub async fn mark_disconnected(&self) {
        let mut state = self.state.lock().await;
        state.health_status = HealthStatus::Disconnected;
        state.connection_start = None;
        state.is_reconnecting = false; // Clear flag so reconnection can be triggered
    }

    /// Mark that reconnection is starting
    pub async fn mark_reconnecting(&self, reason: ReconnectReason) {
        let mut state = self.state.lock().await;
        state.is_reconnecting = true;
        state.health_status = HealthStatus::Reconnecting;
        state.last_reconnect_reason = Some(reason);
        state.last_reconnect_time = Some(Instant::now());
        state.reconnect_attempts += 1;
        state.total_reconnects += 1;
    }

    /// Start reconnecting (without requiring ReconnectReason)
    pub async fn start_reconnecting(&self) {
        let mut state = self.state.lock().await;
        state.is_reconnecting = true;
        state.health_status = HealthStatus::Reconnecting;
    }

    /// Finish reconnecting (clear the flag)
    pub async fn finish_reconnecting(&self) {
        let mut state = self.state.lock().await;
        state.is_reconnecting = false;
    }

    /// Check connection health and return status
    pub async fn check_health(&self, is_connected: bool) -> HealthStatus {
        let mut state = self.state.lock().await;
        let now = Instant::now();

        // If explicitly disconnected, mark as such
        if !is_connected {
            state.health_status = HealthStatus::Disconnected;
            return HealthStatus::Disconnected;
        }

        // If reconnecting, keep that status
        if state.is_reconnecting {
            return HealthStatus::Reconnecting;
        }

        // Check RX timeout
        if let Some(last_rx) = state.last_rx {
            let rx_elapsed = now.duration_since(last_rx);

            if rx_elapsed >= self.config.rx_timeout {
                state.health_status = HealthStatus::Unhealthy;
                return HealthStatus::Unhealthy;
            } else if rx_elapsed >= self.config.rx_timeout * 9 / 10 {
                // Warn at 90% of timeout (less aggressive)
                state.health_status = HealthStatus::Degraded;
                return HealthStatus::Degraded;
            }
        }

        state.health_status = HealthStatus::Healthy;
        HealthStatus::Healthy
    }

    /// Check if reconnection is needed (sync version for non-Tokio thread)
    /// Only checks internal watchdog state, not BLE connection state
    pub async fn check_reconnect_needed_sync(&self, ble: &Arc<Device>) -> Option<String> {
        // Extract needed data without holding the lock across await
        let (is_reconnecting, health_status, last_rx, rx_timeout) = {
            let state = match self.state.try_lock() {
                Ok(s) => s,
                Err(_) => {
                    // If we can't get the lock, skip this check
                    return None;
                }
            };

            (
                state.is_reconnecting,
                state.health_status,
                state.last_rx,
                self.config.rx_timeout,
            )
        }; // MutexGuard dropped here

        // Don't reconnect if already reconnecting
        if is_reconnecting {
            return None;
        }

        if let Ok(false) = ble.is_connected().await {
            return Some(format!("Connection lost (manual check)"));
        }

        // Check internal health status first (set by mark_disconnected)
        if health_status == HealthStatus::Disconnected {
            return Some("Connection lost (marked disconnected)".to_string());
        }

        // Check RX timeout
        if let Some(last_rx) = last_rx {
            let now = Instant::now();
            let elapsed = now.duration_since(last_rx);
            if elapsed >= rx_timeout {
                return Some(format!("No RX traffic for {} seconds", elapsed.as_secs()));
            }
        }

        None
    }

    /// Determine if reconnection is needed and return the reason
    pub async fn should_reconnect(&self, is_connected: bool) -> Option<ReconnectReason> {
        let state = self.state.lock().await;
        let now = Instant::now();

        // Don't reconnect if already reconnecting
        if state.is_reconnecting {
            println!("Don't reconnect if already reconnecting");
            return None;
        }

        // Check internal health status first (set by mark_disconnected)
        if state.health_status == HealthStatus::Disconnected {
            return Some(ReconnectReason::ConnectionLost);
        }

        // Check if BLE connection is lost
        if !is_connected {
            return Some(ReconnectReason::ConnectionLost);
        }

        // Check RX timeout
        if let Some(last_rx) = state.last_rx {
            println!("last_rx: {last_rx:?}");
            let elapsed = now.duration_since(last_rx);
            println!("elapsed: {elapsed:?}");
            if elapsed >= self.config.rx_timeout {
                return Some(ReconnectReason::RxTimeout);
            }
        }

        None
    }

    /// Check if we've exceeded max reconnection attempts
    pub async fn is_max_attempts_exceeded(&self) -> bool {
        let state = self.state.lock().await;
        state.reconnect_attempts >= self.config.max_reconnect_attempts
    }

    /// Get the current backoff duration and advance to next backoff
    pub async fn get_and_advance_backoff(&self) -> Duration {
        let mut state = self.state.lock().await;
        let current = state.current_backoff;

        // Calculate next backoff with exponential increase
        let next = Duration::from_secs_f64(
            (state.current_backoff.as_secs_f64() * self.config.backoff_multiplier)
                .min(self.config.max_backoff.as_secs_f64()),
        );
        state.current_backoff = next;

        current
    }

    /// Get current health metrics
    pub async fn get_metrics(&self) -> WatchdogMetrics {
        let state = self.state.lock().await;
        let now = Instant::now();

        WatchdogMetrics {
            health_status: state.health_status,
            uptime: state
                .connection_start
                .map(|start| now.duration_since(start)),
            last_rx_elapsed: state.last_rx.map(|last| now.duration_since(last)),
            reconnect_attempts: state.reconnect_attempts,
            total_reconnects: state.total_reconnects,
            last_reconnect_reason: state.last_reconnect_reason,
            current_backoff: state.current_backoff,
        }
    }

    /// Run the watchdog monitoring loop
    ///
    /// This will continuously monitor connection health and call the reconnect_callback
    /// when reconnection is needed. The callback should return true if reconnection
    /// was successful, false otherwise.
    pub async fn run_monitoring_loop<F, Fut, G, GFut>(
        &self,
        is_connected_fn: G,
        reconnect_callback: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: Fn(ReconnectReason) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>
            + Send,
        G: Fn() -> GFut + Send + 'static,
        GFut: std::future::Future<Output = bool> + Send,
    {
        let mut last_log = Instant::now();

        loop {
            sleep(self.config.check_interval).await;

            // Check if device is connected
            let is_connected = is_connected_fn().await;

            // Check health
            let health = self.check_health(is_connected).await;

            // Log status periodically (every 5 minutes - not noisy)
            if last_log.elapsed() >= Duration::from_secs(300) {
                let metrics = self.get_metrics().await;
                println!(
                    "   💓 Watchdog status: {} | RX: {:?} ago | Reconnects: {}",
                    health, metrics.last_rx_elapsed, metrics.total_reconnects
                );
                last_log = Instant::now();
            }

            // Check if reconnection is needed
            if let Some(reason) = self.should_reconnect(is_connected).await {
                // Check if we've exceeded max attempts
                if self.is_max_attempts_exceeded().await {
                    eprintln!("⚠️  Maximum reconnection attempts exceeded!");
                    return Err("Maximum reconnection attempts exceeded".into());
                }

                // Get backoff duration
                let backoff = self.get_and_advance_backoff().await;
                let attempts = {
                    let state = self.state.lock().await;
                    state.reconnect_attempts
                };

                eprintln!("\n⚠️  Connection issue detected: {}", reason);
                eprintln!(
                    "   🔄 Attempting reconnection (attempt {}/{})...",
                    attempts, self.config.max_reconnect_attempts
                );
                eprintln!("   ⏱️  Backing off for {:?}", backoff);

                // Mark as reconnecting
                self.mark_reconnecting(reason).await;

                // Apply backoff
                sleep(backoff).await;

                // Attempt reconnection
                match reconnect_callback(reason).await {
                    Ok(_) => {
                        println!("   ✅ Reconnection successful!");
                        self.mark_connected().await;
                    }
                    Err(e) => {
                        eprintln!("   ❌ Reconnection failed: {}", e);
                        self.mark_disconnected().await;

                        // Continue loop to try again
                        continue;
                    }
                }
            } else if health == HealthStatus::Degraded {
                // Warn about degraded health
                let metrics = self.get_metrics().await;
                eprintln!(
                    "⚠️  Connection degraded - RX: {:?} ago",
                    metrics.last_rx_elapsed
                );
            }
        }
    }
}

impl Default for WatchdogManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Health metrics for diagnostics
#[derive(Debug, Clone)]
pub struct WatchdogMetrics {
    pub health_status: HealthStatus,
    pub uptime: Option<Duration>,
    pub last_rx_elapsed: Option<Duration>,
    pub reconnect_attempts: u32,
    pub total_reconnects: u64,
    pub last_reconnect_reason: Option<ReconnectReason>,
    pub current_backoff: Duration,
}

impl std::fmt::Display for WatchdogMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Watchdog Metrics:")?;
        writeln!(f, "  Status: {}", self.health_status)?;
        writeln!(f, "  Uptime: {:?}", self.uptime)?;
        writeln!(f, "  Last RX: {:?} ago", self.last_rx_elapsed)?;
        writeln!(f, "  Reconnect Attempts: {}", self.reconnect_attempts)?;
        writeln!(f, "  Total Reconnects: {}", self.total_reconnects)?;
        writeln!(
            f,
            "  Last Reconnect Reason: {:?}",
            self.last_reconnect_reason
        )?;
        writeln!(f, "  Current Backoff: {:?}", self.current_backoff)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_watchdog_basic() {
        let watchdog = WatchdogManager::new();

        // Initial state should be disconnected
        let health = watchdog.check_health(false).await;
        assert_eq!(health, HealthStatus::Disconnected);

        // Mark connected
        watchdog.mark_connected().await;
        let health = watchdog.check_health(true).await;
        assert_eq!(health, HealthStatus::Healthy);

        // Record traffic
        watchdog.record_rx().await;

        let metrics = watchdog.get_metrics().await;
        assert_eq!(metrics.health_status, HealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_watchdog_timeout() {
        let mut config = WatchdogConfig::default();
        config.rx_timeout = Duration::from_millis(100);

        let watchdog = WatchdogManager::with_config(config);
        watchdog.mark_connected().await;

        // Wait for timeout
        sleep(Duration::from_millis(150)).await;

        // Should detect unhealthy state
        let health = watchdog.check_health(true).await;
        assert_eq!(health, HealthStatus::Unhealthy);

        // Should suggest reconnection
        let reason = watchdog.should_reconnect(true).await;
        assert_eq!(reason, Some(ReconnectReason::RxTimeout));
    }

    #[tokio::test]
    async fn test_watchdog_backoff() {
        let watchdog = WatchdogManager::new();

        // First backoff should be initial
        let backoff1 = watchdog.get_and_advance_backoff().await;
        assert_eq!(backoff1, Duration::from_secs(1));

        // Second should be doubled
        let backoff2 = watchdog.get_and_advance_backoff().await;
        assert_eq!(backoff2, Duration::from_secs(2));

        // Third should be doubled again
        let backoff3 = watchdog.get_and_advance_backoff().await;
        assert_eq!(backoff3, Duration::from_secs(4));
    }
}
