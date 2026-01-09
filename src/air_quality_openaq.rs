//! OpenAQ Air Quality Provider
//!
//! This module provides air quality data from OpenAQ, a global open air quality data platform.
//! OpenAQ provides free air quality data with an API key.
//!
//! # Features
//! - Current air quality measurements from monitoring stations
//! - Multiple pollutant parameters (PM2.5, PM10, O3, NO2, SO2, CO)
//! - Global coverage
//! - Requires free API key from https://openaq.org
//!
//! # Data Sources
//! - Locations: Air quality monitoring stations worldwide
//! - Latest: Most recent measurements from sensors
//! - Format: JSON
//!
//! # Location Mapping
//! OpenAQ uses geographic coordinates with radius search to find nearby stations.
//! The API supports point and radius queries to find stations near a specific location.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Error, Debug)]
pub enum OpenAqError {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("JSON parsing failed: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("No air quality data available near coordinates: lat={0}, lon={1}")]
    NoDataAvailable(f64, f64),

    #[error("API key not configured")]
    NoApiKey,

    #[error("Invalid data format: {0}")]
    InvalidData(String),

    #[error("API error: {0}")]
    ApiError(String),
}

/// OpenAQ API response metadata
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpenAqMeta {
    pub name: String,
    pub website: String,
    pub page: i32,
    pub limit: i32,
    #[serde(deserialize_with = "deserialize_found")]
    pub found: Option<i32>,
}

/// Custom deserializer for the 'found' field which can be an integer or string like ">1"
fn deserialize_found<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Visitor};
    use std::fmt;

    struct FoundVisitor;

    impl<'de> Visitor<'de> for FoundVisitor {
        type Value = Option<i32>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an integer or a string")
        }

        fn visit_i64<E>(self, value: i64) -> Result<Option<i32>, E>
        where
            E: de::Error,
        {
            Ok(Some(value as i32))
        }

        fn visit_u64<E>(self, value: u64) -> Result<Option<i32>, E>
        where
            E: de::Error,
        {
            Ok(Some(value as i32))
        }

        fn visit_str<E>(self, value: &str) -> Result<Option<i32>, E>
        where
            E: de::Error,
        {
            // Handle strings like ">1", "<1000", etc.
            // Try to parse as integer first, otherwise return None
            if let Ok(num) = value.parse::<i32>() {
                Ok(Some(num))
            } else {
                // For strings like ">1", we can't represent them as exact numbers
                // so return None to indicate "many"
                Ok(None)
            }
        }

        fn visit_none<E>(self) -> Result<Option<i32>, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_unit<E>(self) -> Result<Option<i32>, E>
        where
            E: de::Error,
        {
            Ok(None)
        }
    }

    deserializer.deserialize_any(FoundVisitor)
}

/// OpenAQ datetime information (UTC and local)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpenAqDatetime {
    pub utc: String,
    pub local: String,
}

/// OpenAQ coordinates
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpenAqCoordinates {
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

/// OpenAQ latest measurement
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenAqLatest {
    pub datetime: OpenAqDatetime,
    pub value: f64,
    pub coordinates: OpenAqCoordinates,
    pub sensors_id: i32,
    pub locations_id: i32,
}

/// OpenAQ latest response
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpenAqLatestResponse {
    pub meta: OpenAqMeta,
    pub results: Vec<OpenAqLatest>,
}

/// OpenAQ parameter information
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpenAqParameter {
    pub id: i32,
    pub name: String,
    pub units: String,
    #[serde(rename = "displayName")]
    pub display_name: Option<String>,
}

/// OpenAQ sensor information
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpenAqSensor {
    pub id: i32,
    pub name: String,
    pub parameter: OpenAqParameter,
}

/// OpenAQ country information
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpenAqCountry {
    pub id: i32,
    pub code: String,
    pub name: String,
}

/// OpenAQ provider information
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpenAqProvider {
    pub id: i32,
    pub name: String,
}

/// OpenAQ location information
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenAqLocation {
    pub id: i32,
    pub name: String,
    pub locality: Option<String>,
    pub timezone: String,
    pub country: OpenAqCountry,
    pub provider: OpenAqProvider,
    pub coordinates: OpenAqCoordinates,
    pub is_mobile: bool,
    pub is_monitor: bool,
    pub sensors: Option<Vec<OpenAqSensor>>,
}

/// OpenAQ locations response
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpenAqLocationsResponse {
    pub meta: OpenAqMeta,
    pub results: Vec<OpenAqLocation>,
}

/// Unified air quality data
#[derive(Debug, Clone)]
pub struct AirQualityData {
    pub location_id: i32,
    pub location_name: String,
    pub lat: f64,
    pub lon: f64,
    pub datetime: DateTime<Utc>,
    /// PM2.5 in µg/m³
    pub pm25: Option<f64>,
    /// PM10 in µg/m³
    pub pm10: Option<f64>,
    /// Ozone (O3) in ppm
    pub o3: Option<f64>,
    /// Nitrogen Dioxide (NO2) in ppm
    pub no2: Option<f64>,
    /// Sulfur Dioxide (SO2) in ppm
    pub so2: Option<f64>,
    /// Carbon Monoxide (CO) in ppm
    pub co: Option<f64>,
    /// Air Quality Index (calculated from PM2.5 if available)
    pub aqi: Option<i32>,
    pub garmin_aq: Option<i32>,
}

/// Cache entry for air quality data
#[derive(Clone)]
struct CacheEntry {
    data: AirQualityData,
    timestamp: i64,
}

/// OpenAQ service for fetching air quality data
pub struct OpenAqService {
    api_key: Option<String>,
    client: reqwest::Client,
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
    cache_duration: i64,
}

impl OpenAqService {
    const BASE_URL: &'static str = "https://api.openaq.org/v3";
    const DEFAULT_RADIUS: i32 = 10000; // 10km in meters
    const MAX_RADIUS: i32 = 25000; // 25km max allowed by API

    /// Create a new OpenAQ service
    ///
    /// # Arguments
    /// * `api_key` - Optional API key for OpenAQ (get from https://openaq.org)
    /// * `cache_duration` - Cache duration in seconds (default: 600 = 10 minutes)
    pub fn new(api_key: Option<String>, cache_duration: Option<i64>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            api_key,
            client,
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_duration: cache_duration.unwrap_or(600),
        }
    }

    /// Set the API key
    pub fn set_api_key(&mut self, api_key: String) {
        self.api_key = Some(api_key);
    }

    /// Fetch air quality data for a location
    ///
    /// # Arguments
    /// * `lat` - Latitude in degrees
    /// * `lon` - Longitude in degrees
    /// * `radius` - Search radius in meters (default: 10000, max: 25000)
    pub async fn fetch_air_quality(
        &self,
        lat: f64,
        lon: f64,
        radius: Option<i32>,
    ) -> Result<AirQualityData, OpenAqError> {
        let cache_key = format!("{:.4},{:.4}", lat, lon);

        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(&cache_key) {
                let now = chrono::Utc::now().timestamp();
                if now - entry.timestamp < self.cache_duration {
                    log::debug!("OpenAQ: Cache hit for {}", cache_key);
                    return Ok(entry.data.clone());
                }
            }
        }

        log::info!(
            "OpenAQ: Fetching air quality data for lat={}, lon={}",
            lat,
            lon
        );

        // Find nearest location
        let location = self.find_nearest_location(lat, lon, radius).await?;
        log::debug!(
            "OpenAQ: Found location: {} (id={})",
            location.name,
            location.id
        );

        // Fetch latest measurements for this location
        let measurements = self.fetch_location_latest(location.id).await?;

        // Convert to unified air quality data
        let data = self.convert_to_air_quality_data(&location, &measurements)?;

        // Update cache
        {
            let mut cache = self.cache.write().await;
            cache.insert(
                cache_key,
                CacheEntry {
                    data: data.clone(),
                    timestamp: chrono::Utc::now().timestamp(),
                },
            );
        }

        Ok(data)
    }

    /// Find the nearest location to the given coordinates
    async fn find_nearest_location(
        &self,
        lat: f64,
        lon: f64,
        radius: Option<i32>,
    ) -> Result<OpenAqLocation, OpenAqError> {
        let api_key = self.api_key.as_ref().ok_or(OpenAqError::NoApiKey)?;

        let radius = radius
            .unwrap_or(Self::DEFAULT_RADIUS)
            .min(Self::MAX_RADIUS)
            .max(1000);

        let url = format!(
            "{}/locations?coordinates={},{}&radius={}&limit=1",
            Self::BASE_URL,
            lat,
            lon,
            radius
        );

        log::debug!("OpenAQ: Requesting locations: {}", url);

        let response = self
            .client
            .get(&url)
            .header("X-API-Key", api_key)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(OpenAqError::ApiError(format!(
                "API returned status {}: {}",
                status, body
            )));
        }

        let locations: OpenAqLocationsResponse = response.json().await?;

        if locations.results.is_empty() {
            return Err(OpenAqError::NoDataAvailable(lat, lon));
        }

        Ok(locations.results[0].clone())
    }

    /// Fetch latest measurements for a location
    async fn fetch_location_latest(
        &self,
        location_id: i32,
    ) -> Result<Vec<OpenAqLatest>, OpenAqError> {
        let api_key = self.api_key.as_ref().ok_or(OpenAqError::NoApiKey)?;

        let url = format!("{}/locations/{}/latest", Self::BASE_URL, location_id);

        log::debug!("OpenAQ: Requesting latest measurements: {}", url);

        let response = self
            .client
            .get(&url)
            .header("X-API-Key", api_key)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(OpenAqError::ApiError(format!(
                "API returned status {}: {}",
                status, body
            )));
        }

        let latest: OpenAqLatestResponse = response.json().await?;
        Ok(latest.results)
    }

    fn convert_aqi_to_garmin_air_quality(aqi: Option<i32>) -> Option<i32> {
        if let Some(index) = aqi {
            match index {
                0..20 => Some(0),
                20..50 => Some(1),
                50..100 => Some(2),
                100..150 => Some(3),
                150..250 => Some(4),
                _ => Some(5),
            }
        } else {
            None
        }
    }

    /// Convert OpenAQ measurements to unified air quality data
    fn convert_to_air_quality_data(
        &self,
        location: &OpenAqLocation,
        measurements: &[OpenAqLatest],
    ) -> Result<AirQualityData, OpenAqError> {
        if measurements.is_empty() {
            return Err(OpenAqError::InvalidData(
                "No measurements available".to_string(),
            ));
        }

        let lat = location
            .coordinates
            .latitude
            .ok_or_else(|| OpenAqError::InvalidData("Missing latitude".to_string()))?;
        let lon = location
            .coordinates
            .longitude
            .ok_or_else(|| OpenAqError::InvalidData("Missing longitude".to_string()))?;

        // Parse datetime from first measurement
        let datetime = chrono::DateTime::parse_from_rfc3339(&measurements[0].datetime.utc)
            .map_err(|e| OpenAqError::InvalidData(format!("Invalid datetime: {}", e)))?
            .with_timezone(&Utc);

        // Extract pollutant values from measurements
        // We need to match measurements to parameter IDs or names
        let mut pm25: Option<f64> = None;
        let mut pm10: Option<f64> = None;
        let mut o3: Option<f64> = None;
        let mut no2: Option<f64> = None;
        let mut so2: Option<f64> = None;
        let mut co: Option<f64> = None;

        // Map sensor IDs to parameter names using the location's sensor list
        let mut sensor_params = HashMap::new();
        if let Some(sensors) = &location.sensors {
            for sensor in sensors {
                sensor_params.insert(sensor.id, sensor.parameter.name.clone());
            }
        }

        for measurement in measurements {
            if let Some(param_name) = sensor_params.get(&measurement.sensors_id) {
                match param_name.to_lowercase().as_str() {
                    "pm25" | "pm2.5" => pm25 = Some(measurement.value),
                    "pm10" => pm10 = Some(measurement.value),
                    "o3" | "ozone" => o3 = Some(measurement.value),
                    "no2" => no2 = Some(measurement.value),
                    "so2" => so2 = Some(measurement.value),
                    "co" => co = Some(measurement.value),
                    _ => {}
                }
            }
        }

        // Calculate AQI from PM2.5 if available (US EPA AQI)
        let aqi = pm25.map(|pm| Self::calculate_aqi_from_pm25(pm));

        let garmin_aq = Self::convert_aqi_to_garmin_air_quality(aqi);

        Ok(AirQualityData {
            location_id: location.id,
            location_name: location.name.clone(),
            lat,
            lon,
            datetime,
            pm25,
            pm10,
            o3,
            no2,
            so2,
            co,
            aqi,
            garmin_aq,
        })
    }

    /// Calculate US EPA AQI from PM2.5 concentration (µg/m³)
    fn calculate_aqi_from_pm25(pm25: f64) -> i32 {
        // US EPA AQI breakpoints for PM2.5 (24-hour average)
        let breakpoints = [
            (0.0, 12.0, 0, 50),
            (12.1, 35.4, 51, 100),
            (35.5, 55.4, 101, 150),
            (55.5, 150.4, 151, 200),
            (150.5, 250.4, 201, 300),
            (250.5, 350.4, 301, 400),
            (350.5, 500.4, 401, 500),
        ];

        for (c_low, c_high, i_low, i_high) in breakpoints.iter() {
            if pm25 >= *c_low && pm25 <= *c_high {
                let aqi =
                    ((i_high - i_low) as f64 / (c_high - c_low)) * (pm25 - c_low) + (*i_low as f64);
                return aqi.round() as i32;
            }
        }

        // If above all breakpoints, use highest category calculation
        if pm25 > 500.4 {
            return 500;
        }

        0 // Should not reach here for valid inputs
    }

    /// Clear the cache
    pub async fn clear_cache(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }

    /// Get cache statistics
    pub async fn cache_stats(&self) -> (usize, Vec<(String, i64)>) {
        let cache = self.cache.read().await;
        let size = cache.len();
        let entries: Vec<(String, i64)> = cache
            .iter()
            .map(|(key, entry)| (key.clone(), entry.timestamp))
            .collect();
        (size, entries)
    }
}

/// Get AQI category description
pub fn get_aqi_category(aqi: i32) -> &'static str {
    match aqi {
        0..=50 => "Good",
        51..=100 => "Moderate",
        101..=150 => "Unhealthy for Sensitive Groups",
        151..=200 => "Unhealthy",
        201..=300 => "Very Unhealthy",
        301..=500 => "Hazardous",
        _ => "Beyond AQI",
    }
}

/// Get AQI color code (for UI display)
pub fn get_aqi_color(aqi: i32) -> &'static str {
    match aqi {
        0..=50 => "#00E400",    // Green
        51..=100 => "#FFFF00",  // Yellow
        101..=150 => "#FF7E00", // Orange
        151..=200 => "#FF0000", // Red
        201..=300 => "#8F3F97", // Purple
        301..=500 => "#7E0023", // Maroon
        _ => "#7E0023",         // Maroon
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aqi_calculation() {
        // Test AQI breakpoints
        assert_eq!(OpenAqService::calculate_aqi_from_pm25(0.0), 0);
        assert_eq!(OpenAqService::calculate_aqi_from_pm25(12.0), 50);
        assert_eq!(OpenAqService::calculate_aqi_from_pm25(35.4), 100);
        assert_eq!(OpenAqService::calculate_aqi_from_pm25(55.4), 150);
        assert_eq!(OpenAqService::calculate_aqi_from_pm25(150.4), 200);
        assert_eq!(OpenAqService::calculate_aqi_from_pm25(250.4), 300);
        assert_eq!(OpenAqService::calculate_aqi_from_pm25(350.4), 400);
        assert_eq!(OpenAqService::calculate_aqi_from_pm25(500.4), 500);

        // Test mid-range values
        let aqi_25 = OpenAqService::calculate_aqi_from_pm25(25.0);
        assert!(aqi_25 >= 51 && aqi_25 <= 100);

        let aqi_100 = OpenAqService::calculate_aqi_from_pm25(100.0);
        assert!(aqi_100 >= 151 && aqi_100 <= 200);
    }

    #[test]
    fn test_aqi_categories() {
        assert_eq!(get_aqi_category(25), "Good");
        assert_eq!(get_aqi_category(75), "Moderate");
        assert_eq!(get_aqi_category(125), "Unhealthy for Sensitive Groups");
        assert_eq!(get_aqi_category(175), "Unhealthy");
        assert_eq!(get_aqi_category(250), "Very Unhealthy");
        assert_eq!(get_aqi_category(400), "Hazardous");
    }

    #[test]
    fn test_aqi_colors() {
        assert_eq!(get_aqi_color(25), "#00E400");
        assert_eq!(get_aqi_color(75), "#FFFF00");
        assert_eq!(get_aqi_color(125), "#FF7E00");
        assert_eq!(get_aqi_color(175), "#FF0000");
        assert_eq!(get_aqi_color(250), "#8F3F97");
        assert_eq!(get_aqi_color(400), "#7E0023");
    }

    #[tokio::test]
    async fn test_service_creation() {
        let service = OpenAqService::new(None, Some(600));
        assert!(service.api_key.is_none());

        let service_with_key = OpenAqService::new(Some("test-key".to_string()), Some(600));
        assert!(service_with_key.api_key.is_some());
    }

    #[tokio::test]
    async fn test_cache_operations() {
        let service = OpenAqService::new(None, Some(600));

        // Clear cache
        service.clear_cache().await;

        // Check stats
        let (size, _entries) = service.cache_stats().await;
        assert_eq!(size, 0);
    }
}
