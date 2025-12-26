//! Australian Bureau of Meteorology (BOM) Weather Provider
//!
//! This module provides weather data from the Australian Bureau of Meteorology.
//! BOM provides free weather data without requiring an API key.
//!
//! # Features
//! - No API key required
//! - Current observations from weather stations
//! - 7-day forecasts
//! - Free and open data
//!
//! # Data Sources
//! - Observations: Individual weather station data
//! - Forecasts: City and town forecasts
//! - Format: JSON (preferred) and XML
//!
//! # Location Mapping
//! BOM uses location IDs rather than lat/lon coordinates. This module includes
//! major Australian cities and can be extended with additional locations.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

/// Encode latitude/longitude to geohash with precision 6
fn encode_geohash(lat: f64, lon: f64) -> String {
    const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    const PRECISION: usize = 6;

    let mut geohash = String::new();
    let mut lat_range = (-90.0, 90.0);
    let mut lon_range = (-180.0, 180.0);
    let mut is_even = true;
    let mut bit = 0;
    let mut ch = 0;

    while geohash.len() < PRECISION {
        if is_even {
            let mid = (lon_range.0 + lon_range.1) / 2.0;
            if lon > mid {
                ch |= 1 << (4 - bit);
                lon_range.0 = mid;
            } else {
                lon_range.1 = mid;
            }
        } else {
            let mid = (lat_range.0 + lat_range.1) / 2.0;
            if lat > mid {
                ch |= 1 << (4 - bit);
                lat_range.0 = mid;
            } else {
                lat_range.1 = mid;
            }
        }

        is_even = !is_even;

        if bit < 4 {
            bit += 1;
        } else {
            geohash.push(BASE32[ch as usize] as char);
            bit = 0;
            ch = 0;
        }
    }

    geohash
}

#[derive(Error, Debug)]
pub enum BomWeatherError {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("JSON parsing failed: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Location not found for coordinates: lat={0}, lon={1}")]
    LocationNotFound(f64, f64),

    #[error("No weather data available for location: {0}")]
    NoDataAvailable(String),

    #[error("Invalid data format: {0}")]
    InvalidData(String),
}

/// BOM Location information
#[derive(Debug, Clone)]
pub struct BomLocation {
    pub name: String,
    pub state: String,
    pub lat: f64,
    pub lon: f64,
    /// Weather station ID (for observations)
    pub wmo_id: String,
    /// Forecast area code (for forecasts)
    pub aac: String,
    /// Geohash for BOM API v1 (for hourly/daily forecasts)
    pub geohash: String,
}

/// BOM Observation data (current conditions)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BomObservation {
    #[serde(rename = "sort_order")]
    pub sort_order: Option<i32>,
    #[serde(rename = "wmo")]
    pub wmo: Option<i32>,
    pub name: String,
    #[serde(rename = "history_product")]
    pub history_product: Option<String>,
    #[serde(rename = "local_date_time")]
    pub local_date_time: Option<String>,
    #[serde(rename = "local_date_time_full")]
    pub local_date_time_full: Option<String>,
    #[serde(rename = "aifstime_utc")]
    pub aifstime_utc: Option<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    #[serde(rename = "apparent_t")]
    pub apparent_t: Option<f64>,
    pub cloud: Option<String>,
    #[serde(rename = "cloud_base_m")]
    pub cloud_base_m: Option<i32>,
    #[serde(rename = "cloud_oktas")]
    pub cloud_oktas: Option<i32>,
    #[serde(rename = "cloud_type_id")]
    pub cloud_type_id: Option<i32>,
    #[serde(rename = "cloud_type")]
    pub cloud_type: Option<String>,
    #[serde(rename = "delta_t")]
    pub delta_t: Option<f64>,
    pub gust_kmh: Option<f64>,
    #[serde(rename = "gust_kt")]
    pub gust_kt: Option<f64>,
    pub air_temp: Option<f64>,
    pub dewpt: Option<f64>,
    pub press: Option<f64>,
    #[serde(rename = "press_qnh")]
    pub press_qnh: Option<f64>,
    #[serde(rename = "press_msl")]
    pub press_msl: Option<f64>,
    #[serde(rename = "press_tend")]
    pub press_tend: Option<String>,
    #[serde(rename = "rain_trace")]
    pub rain_trace: Option<String>,
    pub rel_hum: Option<i32>,
    pub sea_state: Option<String>,
    #[serde(rename = "swell_dir_worded")]
    pub swell_dir_worded: Option<String>,
    #[serde(rename = "swell_height")]
    pub swell_height: Option<f64>,
    #[serde(rename = "swell_period")]
    pub swell_period: Option<f64>,
    pub vis_km: Option<String>,
    pub weather: Option<String>,
    #[serde(rename = "wind_dir")]
    pub wind_dir: Option<String>,
    #[serde(rename = "wind_spd_kmh")]
    pub wind_spd_kmh: Option<f64>,
    #[serde(rename = "wind_spd_kt")]
    pub wind_spd_kt: Option<f64>,
}

/// BOM Observations response
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BomObservationsData {
    pub observations: ObservationsWrapper,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ObservationsWrapper {
    pub notice: Option<Vec<Notice>>,
    pub header: Option<Vec<Header>>,
    pub data: Vec<BomObservation>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Notice {
    pub copyright: Option<String>,
    pub copyright_url: Option<String>,
    pub disclaimer_url: Option<String>,
    pub feedback_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Header {
    pub refresh_message: Option<String>,
    #[serde(rename = "ID")]
    pub id: Option<String>,
    pub main_id: Option<String>,
    pub name: Option<String>,
    pub state_time_zone: Option<String>,
    pub time_zone: Option<String>,
    pub product_name: Option<String>,
    pub state: Option<String>,
}

/// BOM Forecast data (from précis forecasts)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BomForecast {
    pub index: i32,
    pub product_id: String,
    pub aac: String,
    pub name: String,
    pub state: String,
    pub forecast_period: String,
    pub start_time_local: String,
    pub end_time_local: String,
    pub minimum_temperature: Option<f64>,
    pub maximum_temperature: Option<f64>,
    pub precis: String,
    pub probability_of_precipitation: Option<String>,
    pub icon_descriptor: Option<String>,
}

/// Unified weather data structure (compatible with existing system)
#[derive(Debug, Clone)]
pub struct BomWeatherData {
    pub lat: f64,
    pub lon: f64,
    pub location_name: String,
    pub current: BomCurrentWeather,
    pub hourly: Vec<BomHourlyWeather>,
    pub daily: Vec<BomDailyWeather>,
}

#[derive(Debug, Clone)]
pub struct BomCurrentWeather {
    pub dt: i64,
    pub temp: f64,
    pub feels_like: f64,
    pub pressure: i32,
    pub humidity: i32,
    pub dew_point: f64,
    pub clouds: i32,
    pub visibility: Option<i32>,
    pub wind_speed: f64,
    pub wind_deg: i32,
    pub condition_code: i32,
    pub description: String,
}

#[derive(Debug, Clone)]
pub struct BomHourlyWeather {
    pub dt: i64,
    pub temp: f64,
    pub feels_like: f64,
    pub humidity: i32,
    pub wind_speed: f64,
    pub wind_deg: i32,
    pub pop: f64,
    pub condition_code: i32,
    pub description: String,
    pub uv_index: Option<f32>,
    pub dew_point: Option<f64>,
    pub pressure: Option<i32>,
    pub visibility: Option<i32>,
    pub clouds: Option<i32>,
    pub air_quality: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct BomDailyWeather {
    pub dt: i64,
    pub temp_min: f64,
    pub temp_max: f64,
    pub condition_code: i32,
    pub description: String,
    pub pop: f64,        // Probability of precipitation
    pub wind_speed: f64, // m/s
    pub wind_deg: i32,
    pub humidity: i32,
    pub sunrise: Option<i64>,
    pub sunset: Option<i64>,
    pub uv_max: Option<f32>,
}

/// Cached BOM weather data
#[derive(Debug, Clone)]
struct CachedBomWeather {
    data: BomWeatherData,
    fetched_at: DateTime<Utc>,
}

/// BOM Weather Service
pub struct BomWeatherService {
    cache: Arc<RwLock<HashMap<(i32, i32), CachedBomWeather>>>,
    cache_duration_secs: i64,
    client: reqwest::Client,
    locations: HashMap<String, BomLocation>,
}

impl BomWeatherService {
    /// Create a new BOM weather service
    ///
    /// # Arguments
    /// * `cache_duration_secs` - How long to cache weather data (default: 600 seconds / 10 minutes)
    pub fn new(cache_duration_secs: Option<i64>) -> Self {
        let mut service = Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_duration_secs: cache_duration_secs.unwrap_or(600),
            client: reqwest::Client::new(),
            locations: HashMap::new(),
        };

        // Initialize with major Australian cities
        service.init_locations();
        service
    }

    /// Initialize location database with major Australian cities
    fn init_locations(&mut self) {
        // Major capital cities and their BOM identifiers
        let locations = vec![
            BomLocation {
                name: "Sydney".to_string(),
                state: "NSW".to_string(),
                lat: -33.8688,
                lon: 151.2093,
                wmo_id: "94768".to_string(), // Sydney Observatory Hill
                aac: "NSW_PT131".to_string(),
                geohash: "r3gx2f".to_string(), // Sydney
            },
            BomLocation {
                name: "Melbourne".to_string(),
                state: "VIC".to_string(),
                lat: -37.8136,
                lon: 144.9631,
                wmo_id: "94866".to_string(), // Melbourne
                aac: "VIC_PT042".to_string(),
                geohash: "r1r0f9".to_string(), // Melbourne
            },
            BomLocation {
                name: "Brisbane".to_string(),
                state: "QLD".to_string(),
                lat: -27.4698,
                lon: 153.0251,
                wmo_id: "94576".to_string(), // Brisbane
                aac: "QLD_PT001".to_string(),
                geohash: "r6dp23".to_string(), // Brisbane
            },
            BomLocation {
                name: "Perth".to_string(),
                state: "WA".to_string(),
                lat: -31.9505,
                lon: 115.8605,
                wmo_id: "94608".to_string(), // Perth Airport
                aac: "WA_PT001".to_string(),
                geohash: "qd66kq".to_string(), // Perth
            },
            BomLocation {
                name: "Adelaide".to_string(),
                state: "SA".to_string(),
                lat: -34.9285,
                lon: 138.6007,
                wmo_id: "94672".to_string(), // Adelaide
                aac: "SA_PT001".to_string(),
                geohash: "r1dqn6".to_string(), // Adelaide
            },
            BomLocation {
                name: "Canberra".to_string(),
                state: "ACT".to_string(),
                lat: -35.2809,
                lon: 149.1300,
                wmo_id: "94926".to_string(), // Canberra Airport
                aac: "ACT_PT001".to_string(),
                geohash: "r3dp6h".to_string(), // Canberra
            },
            BomLocation {
                name: "Hobart".to_string(),
                state: "TAS".to_string(),
                lat: -42.8821,
                lon: 147.3272,
                wmo_id: "94970".to_string(), // Hobart
                aac: "TAS_PT001".to_string(),
                geohash: "r0m6zs".to_string(), // Hobart
            },
            BomLocation {
                name: "Darwin".to_string(),
                state: "NT".to_string(),
                lat: -12.4634,
                lon: 130.8456,
                wmo_id: "94120".to_string(), // Darwin Airport
                aac: "NT_PT001".to_string(),
                geohash: "rqeq6h".to_string(), // Darwin
            },
        ];

        for loc in locations {
            self.locations.insert(loc.wmo_id.clone(), loc);
        }
    }

    /// Find nearest location to given coordinates
    fn find_nearest_location(&self, lat: f64, lon: f64) -> Option<&BomLocation> {
        let mut nearest: Option<(&BomLocation, f64)> = None;

        for location in self.locations.values() {
            let distance = ((location.lat - lat).powi(2) + (location.lon - lon).powi(2)).sqrt();

            if let Some((_, min_dist)) = nearest {
                if distance < min_dist {
                    nearest = Some((location, distance));
                }
            } else {
                nearest = Some((location, distance));
            }
        }

        nearest.map(|(loc, _)| loc)
    }

    /// Convert Garmin semicircles to decimal degrees
    pub fn semicircles_to_degrees(semicircles: i32) -> f64 {
        semicircles as f64 * (180.0 / 2147483648.0)
    }

    /// Fetch weather data for given coordinates
    ///
    /// # Arguments
    /// * `lat_semicircles` - Latitude in Garmin semicircles
    /// * `lon_semicircles` - Longitude in Garmin semicircles
    ///
    /// # Returns
    /// Weather data from BOM (cached if available and fresh)
    pub async fn fetch_weather(
        &self,
        lat_semicircles: i32,
        lon_semicircles: i32,
    ) -> Result<BomWeatherData, BomWeatherError> {
        // Check cache first
        let cache_key = (lat_semicircles, lon_semicircles);
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.get(&cache_key) {
                let age = Utc::now()
                    .signed_duration_since(cached.fetched_at)
                    .num_seconds();
                if age < self.cache_duration_secs {
                    log::debug!(
                        "Using cached BOM weather data (age: {}s, max: {}s)",
                        age,
                        self.cache_duration_secs
                    );
                    return Ok(cached.data.clone());
                }
            }
        }

        // Convert coordinates
        let lat = Self::semicircles_to_degrees(lat_semicircles);
        let lon = Self::semicircles_to_degrees(lon_semicircles);

        log::info!(
            "Fetching BOM weather for coordinates: lat={:.4}°, lon={:.4}°",
            lat,
            lon
        );

        // Find nearest location
        let location = self
            .find_nearest_location(lat, lon)
            .ok_or_else(|| BomWeatherError::LocationNotFound(lat, lon))?;

        // Calculate distance for logging
        let distance = ((location.lat - lat).powi(2) + (location.lon - lon).powi(2)).sqrt();
        let distance_km = distance * 111.0; // Rough conversion to km

        // Calculate geohash for the actual requested coordinates
        let actual_geohash = encode_geohash(lat, lon);

        log::info!(
            "Using BOM location: {} ({}), station ID: {}, location geohash: {}",
            location.name,
            location.state,
            location.wmo_id,
            location.geohash
        );
        log::info!(
            "Distance from requested coords: {:.1} km (requested: {:.4}°, {:.4}° -> using: {:.4}°, {:.4}°)",
            distance_km,
            lat,
            lon,
            location.lat,
            location.lon
        );
        log::info!(
            "Using calculated geohash: {} (from exact coordinates)",
            actual_geohash
        );

        // Fetch current observations
        let observations = self.fetch_observations(location).await?;

        // Fetch daily forecast data first (needed for sunrise/sunset times)
        let daily_forecasts = self
            .fetch_daily_forecasts_with_geohash(&actual_geohash)
            .await?;

        // Fetch hourly forecast data from BOM API using calculated geohash
        // Pass daily forecasts to get sunrise/sunset info for nighttime detection
        let (hourly_forecasts, current_icon_descriptor) = self
            .fetch_hourly_forecasts_with_geohash(&actual_geohash)
            .await?;

        // Parse weather data with forecasts
        let weather_data = self.parse_weather_data(
            location,
            observations,
            &current_icon_descriptor,
            hourly_forecasts,
            daily_forecasts,
        )?;

        // Cache the result
        {
            let mut cache = self.cache.write().await;
            cache.insert(
                cache_key,
                CachedBomWeather {
                    data: weather_data.clone(),
                    fetched_at: Utc::now(),
                },
            );
        }

        log::info!("BOM weather data fetched and cached successfully");
        Ok(weather_data)
    }

    /// Fetch observations from BOM
    async fn fetch_observations(
        &self,
        location: &BomLocation,
    ) -> Result<BomObservationsData, BomWeatherError> {
        // BOM observations JSON URL format varies by state
        // Format: http://www.bom.gov.au/fwo/{PRODUCT_ID}/{PRODUCT_ID}.{WMO_ID}.json
        let product_id = match location.state.as_str() {
            "NSW" => "IDN60901",
            "VIC" => "IDV60901",
            "QLD" => "IDQ60901",
            "WA" => "IDW60901",
            "SA" => "IDS60901",
            "TAS" => "IDT60901",
            "ACT" => "IDN60903", // ACT uses NSW products
            "NT" => "IDD60901",
            _ => "IDN60901", // Default to NSW
        };

        let url = format!(
            "http://www.bom.gov.au/fwo/{}/{}.{}.json",
            product_id, product_id, location.wmo_id
        );

        log::debug!("Fetching BOM observations from: {}", url);

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(BomWeatherError::NoDataAvailable(format!(
                "HTTP {} Not Found for station {}",
                response.status(),
                location.wmo_id
            )));
        }

        let obs_data: BomObservationsData = response.json().await?;
        Ok(obs_data)
    }

    /// Fetch hourly forecasts from BOM API v1 using a specific geohash
    async fn fetch_hourly_forecasts_with_geohash(
        &self,
        geohash: &str,
    ) -> Result<(Vec<BomHourlyWeather>, String), BomWeatherError> {
        let url = format!(
            "https://api.weather.bom.gov.au/v1/locations/{}/forecasts/hourly",
            geohash
        );

        log::debug!("Fetching BOM hourly forecasts from: {}", url);

        match self.client.get(&url).send().await {
            Ok(response) if response.status().is_success() => {
                match response.json::<serde_json::Value>().await {
                    Ok(data) => {
                        return self.parse_hourly_forecast_data(data);
                    }
                    Err(e) => {
                        log::warn!("Failed to parse hourly forecast JSON: {}", e);
                        return Ok((Vec::new(), "".into()));
                    }
                }
            }
            Ok(response) => {
                log::warn!("BOM API returned status {}", response.status());
                return Ok((Vec::new(), "".into()));
            }
            Err(e) => {
                log::warn!("Failed to fetch hourly forecasts: {}", e);
                return Ok((Vec::new(), "".into()));
            }
        }
    }

    /// Parse hourly forecast data from BOM API
    fn parse_hourly_forecast_data(
        &self,
        data: serde_json::Value,
    ) -> Result<(Vec<BomHourlyWeather>, String), BomWeatherError> {
        let mut hourly = Vec::new();
        let mut current_icon_descriptor = "";

        if let Some(forecasts) = data.get("data").and_then(|d| d.as_array()) {
            log::info!("BOM API returned {} hourly forecasts", forecasts.len());

            // Get current time to filter out past forecasts
            let now = Utc::now().timestamp();

            // Log all forecast times to identify any gaps or cutoffs
            if forecasts.len() > 0 {
                log::info!("Forecast time range:");
                if let Some(first) = forecasts.first() {
                    if let Some(first_time) = first.get("time").and_then(|t| t.as_str()) {
                        log::info!("  First: {}", first_time);
                    }
                }
                if let Some(last) = forecasts.last() {
                    if let Some(last_time) = last.get("time").and_then(|t| t.as_str()) {
                        log::info!("  Last: {}", last_time);
                    }
                }
            }

            for forecast in forecasts.iter() {
                // Get time
                let time_str = forecast
                    .get("time")
                    .and_then(|t| t.as_str())
                    .unwrap_or("unknown");

                let dt = forecast
                    .get("time")
                    .and_then(|t| t.as_str())
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.timestamp())
                    .unwrap_or(0);

                // Skip past forecasts - only include current hour or future
                if dt < now {
                    // Save the most recenty icon description to use for the
                    // current observation
                    current_icon_descriptor = forecast
                        .get("icon_descriptor")
                        .and_then(|i| i.as_str())
                        .unwrap_or("clear");
                    continue;
                }

                // Stop after we have 24 future forecasts
                if hourly.len() >= 24 {
                    break;
                }

                // Log first few future forecasts
                if hourly.len() < 6 {
                    log::debug!(
                        "Future hourly forecast {}: time={}, timestamp={}, temp={}, icon={}",
                        hourly.len(),
                        time_str,
                        dt,
                        forecast.get("temp").and_then(|t| t.as_f64()).unwrap_or(0.0),
                        forecast
                            .get("icon_descriptor")
                            .and_then(|i| i.as_str())
                            .unwrap_or("unknown")
                    );
                }

                // Get temperature
                let temp = forecast
                    .get("temp")
                    .and_then(|t| t.as_f64())
                    .unwrap_or(20.0);

                let feels_like = forecast
                    .get("temp_feels_like")
                    .and_then(|t| t.as_f64())
                    .unwrap_or(temp);

                let humidity = forecast
                    .get("humidity")
                    .and_then(|h| h.as_i64())
                    .unwrap_or(50) as i32;

                let wind_speed = forecast
                    .get("wind")
                    .and_then(|w| w.get("speed_kilometre"))
                    .and_then(|s| s.as_f64())
                    .unwrap_or(0.0)
                    / 3.6; // Convert km/h to m/s

                let wind_deg = forecast
                    .get("wind")
                    .and_then(|w| w.get("direction"))
                    .and_then(|d| d.as_str())
                    .and_then(|s| Self::parse_wind_direction_string(s))
                    .unwrap_or(0);

                let pop = forecast
                    .get("rain")
                    .and_then(|r| r.get("chance"))
                    .and_then(|c| c.as_f64())
                    .unwrap_or(0.0)
                    / 100.0;

                let icon_descriptor = forecast
                    .get("icon_descriptor")
                    .and_then(|i| i.as_str())
                    .unwrap_or("clear");

                let is_night = forecast
                    .get("is_night")
                    .and_then(|t| t.as_bool())
                    .unwrap_or(false);

                let condition_code = Self::map_icon_descriptor_to_garmin(icon_descriptor, is_night);

                // Use Garmin-style description based on condition code for consistency
                let description = Self::garmin_condition_to_description(condition_code);

                // Debug: log first forecast to see available fields
                if hourly.len() == 0 {
                    log::info!(
                        "First hourly forecast JSON: {}",
                        serde_json::to_string_pretty(forecast)
                            .unwrap_or_else(|_| "error".to_string())
                    );
                }

                // UV index not available in BOM JSON API hourly forecasts
                // Would need to fetch from separate XML product files
                let uv_index = None;

                // Extract wind gust if available
                let _wind_gust = forecast
                    .get("wind")
                    .and_then(|w| w.get("gust_kilometre"))
                    .and_then(|g| g.as_f64())
                    .map(|g| g / 3.6); // Convert km/h to m/s

                // BOM hourly forecasts don't provide these fields in the JSON API
                // They would need to come from current observations or be interpolated
                let dew_point = None;
                let pressure = None;
                let visibility = None;
                let clouds = None;
                let air_quality = None;

                hourly.push(BomHourlyWeather {
                    dt,
                    temp,
                    feels_like,
                    humidity,
                    wind_speed,
                    wind_deg,
                    pop,
                    condition_code,
                    description,
                    uv_index,
                    dew_point,
                    pressure,
                    visibility,
                    clouds,
                    air_quality,
                });
            }

            log::info!(
                "Successfully parsed {} future hourly forecasts (filtered from {} total)",
                hourly.len(),
                forecasts.len()
            );
        } else {
            log::warn!("No 'data' array found in BOM hourly forecast response");
        }

        Ok((hourly, current_icon_descriptor.into()))
    }

    /// Parse wind direction string to degrees
    fn parse_wind_direction_string(dir: &str) -> Option<i32> {
        match dir {
            "N" => Some(0),
            "NNE" => Some(22),
            "NE" => Some(45),
            "ENE" => Some(67),
            "E" => Some(90),
            "ESE" => Some(112),
            "SE" => Some(135),
            "SSE" => Some(157),
            "S" => Some(180),
            "SSW" => Some(202),
            "SW" => Some(225),
            "WSW" => Some(247),
            "W" => Some(270),
            "WNW" => Some(292),
            "NW" => Some(315),
            "NNW" => Some(337),
            "CALM" => Some(0),
            _ => None,
        }
    }

    /// Fetch daily forecasts from BOM API v1 using a specific geohash
    async fn fetch_daily_forecasts_with_geohash(
        &self,
        geohash: &str,
    ) -> Result<Vec<BomDailyWeather>, BomWeatherError> {
        let url = format!(
            "https://api.weather.bom.gov.au/v1/locations/{}/forecasts/daily",
            geohash
        );

        log::debug!("Fetching BOM daily forecasts from: {}", url);

        match self.client.get(&url).send().await {
            Ok(response) if response.status().is_success() => {
                match response.json::<serde_json::Value>().await {
                    Ok(data) => {
                        return self.parse_daily_forecast_data(data);
                    }
                    Err(e) => {
                        log::warn!("Failed to parse daily forecast JSON: {}", e);
                        return Ok(Vec::new());
                    }
                }
            }
            Ok(response) => {
                log::warn!("BOM API returned status {}", response.status());
                return Ok(Vec::new());
            }
            Err(e) => {
                log::warn!("Failed to fetch daily forecasts: {}", e);
                return Ok(Vec::new());
            }
        }
    }

    /// Parse daily forecast data from BOM API
    fn parse_daily_forecast_data(
        &self,
        data: serde_json::Value,
    ) -> Result<Vec<BomDailyWeather>, BomWeatherError> {
        let mut daily = Vec::new();

        if let Some(forecasts) = data.get("data").and_then(|d| d.as_array()) {
            let base_date = Utc::now().date_naive();

            for (index, forecast) in forecasts.iter().take(7).enumerate() {
                // Get date - use index to calculate if parsing fails
                let dt = if let Some(date_str) = forecast.get("date").and_then(|d| d.as_str()) {
                    log::debug!("Parsing BOM date: {}", date_str);

                    if let Ok(parsed_date) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                    {
                        if let Some(datetime) = parsed_date.and_hms_opt(12, 0, 0) {
                            let timestamp = datetime.and_utc().timestamp();
                            log::debug!("  Parsed to timestamp: {}", timestamp);
                            timestamp
                        } else {
                            log::warn!("Failed to create time for date: {}", date_str);
                            (base_date + chrono::Duration::days(index as i64))
                                .and_hms_opt(12, 0, 0)
                                .unwrap()
                                .and_utc()
                                .timestamp()
                        }
                    } else {
                        log::warn!("Failed to parse date: {}, using index-based date", date_str);
                        (base_date + chrono::Duration::days(index as i64))
                            .and_hms_opt(12, 0, 0)
                            .unwrap()
                            .and_utc()
                            .timestamp()
                    }
                } else {
                    log::warn!("No date field found in forecast, using index {}", index);
                    (base_date + chrono::Duration::days(index as i64))
                        .and_hms_opt(12, 0, 0)
                        .unwrap()
                        .and_utc()
                        .timestamp()
                };

                let temp_min = forecast
                    .get("temp_min")
                    .and_then(|t| t.as_f64())
                    .unwrap_or(15.0);

                let temp_max = forecast
                    .get("temp_max")
                    .and_then(|t| t.as_f64())
                    .unwrap_or(25.0);

                let humidity = forecast
                    .get("humidity")
                    .and_then(|h| h.get("max"))
                    .and_then(|m| m.as_i64())
                    .unwrap_or(50) as i32;

                let wind_speed = forecast
                    .get("wind")
                    .and_then(|w| w.get("speed_kilometre"))
                    .and_then(|s| s.as_f64())
                    .unwrap_or(0.0)
                    / 3.6; // Convert km/h to m/s

                let wind_deg = forecast
                    .get("wind")
                    .and_then(|w| w.get("direction"))
                    .and_then(|d| d.as_str())
                    .and_then(|s| Self::parse_wind_direction_string(s))
                    .unwrap_or(0);

                let pop = forecast
                    .get("rain")
                    .and_then(|r| r.get("chance"))
                    .and_then(|c| c.as_f64())
                    .unwrap_or(0.0)
                    / 100.0;

                let icon_descriptor = forecast
                    .get("icon_descriptor")
                    .and_then(|i| i.as_str())
                    .unwrap_or("clear");

                if index < 3 {
                    log::debug!(
                        "Daily forecast {}: date={}, icon={}, temp_min={}, temp_max={}",
                        index,
                        forecast
                            .get("date")
                            .and_then(|d| d.as_str())
                            .unwrap_or("unknown"),
                        icon_descriptor,
                        temp_min,
                        temp_max
                    );
                }

                let condition_code = Self::map_icon_descriptor_to_garmin(icon_descriptor, false);

                let description = forecast
                    .get("extended_text")
                    .and_then(|e| e.as_str())
                    .unwrap_or(icon_descriptor)
                    .to_string();

                let sunrise = forecast
                    .get("astronomical")
                    .and_then(|a| a.get("sunrise_time"))
                    .and_then(|s| s.as_str())
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.timestamp());

                let sunset = forecast
                    .get("astronomical")
                    .and_then(|a| a.get("sunset_time"))
                    .and_then(|s| s.as_str())
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.timestamp());

                // Try to get UV max from forecast data
                let uv_max = forecast
                    .get("uv")
                    .and_then(|u| u.get("max_index"))
                    .and_then(|m| m.as_f64())
                    .map(|u| u as f32);

                if index < 3 {
                    log::debug!("Daily forecast {} UV max: {:?}", index, uv_max);
                }

                daily.push(BomDailyWeather {
                    dt,
                    temp_min,
                    temp_max,
                    condition_code,
                    description,
                    pop,
                    wind_speed,
                    wind_deg,
                    humidity,
                    sunrise,
                    sunset,
                    uv_max,
                });
            }
        }

        Ok(daily)
    }

    /// Parse BOM data into unified weather structure
    fn parse_weather_data(
        &self,
        location: &BomLocation,
        obs_data: BomObservationsData,
        current_icon_descriptor: &str,
        hourly_forecasts: Vec<BomHourlyWeather>,
        daily_forecasts: Vec<BomDailyWeather>,
    ) -> Result<BomWeatherData, BomWeatherError> {
        let latest_obs =
            obs_data
                .observations
                .data
                .first()
                .ok_or(BomWeatherError::NoDataAvailable(
                    "No observation data available".to_string(),
                ))?;

        // Parse timestamp
        let dt = Utc::now().timestamp();

        // Build current weather
        let current = BomCurrentWeather {
            dt,
            temp: latest_obs.air_temp.unwrap_or(20.0),
            feels_like: latest_obs.apparent_t.unwrap_or(20.0),
            pressure: latest_obs.press_qnh.unwrap_or(1013.0) as i32,
            humidity: latest_obs.rel_hum.unwrap_or(50),
            dew_point: latest_obs.dewpt.unwrap_or(15.0),
            clouds: Self::parse_cloud_coverage(latest_obs.cloud_oktas),
            visibility: Self::parse_visibility(&latest_obs.vis_km),
            wind_speed: latest_obs.wind_spd_kmh.unwrap_or(0.0).max(0.0) / 3.6, // Convert to m/s
            wind_deg: Self::parse_wind_direction(&latest_obs.wind_dir),
            condition_code: Self::map_icon_descriptor_to_garmin(&current_icon_descriptor, false),
            description: current_icon_descriptor.into(),
        };

        // Use provided hourly forecasts, or empty if unavailable
        let hourly = if !hourly_forecasts.is_empty() {
            hourly_forecasts
        } else {
            log::warn!("No hourly forecasts available from BOM API");
            Vec::new()
        };

        // Use provided daily forecasts, or generate fallback if empty
        let daily: Vec<BomDailyWeather> = if !daily_forecasts.is_empty() {
            daily_forecasts
        } else {
            log::warn!("No daily forecasts available, generating fallback");
            self.generate_daily_forecasts(&current, current_icon_descriptor, dt)
        };

        Ok(BomWeatherData {
            lat: location.lat,
            lon: location.lon,
            location_name: location.name.clone(),
            current,
            hourly,
            daily,
        })
    }

    /// Generate fallback daily forecasts (only used if API fails)
    fn generate_daily_forecasts(
        &self,
        current: &BomCurrentWeather,
        description: &str,
        dt: i64,
    ) -> Vec<BomDailyWeather> {
        (0..5)
            .map(|day| {
                let forecast_dt = dt + (day * 86400);
                BomDailyWeather {
                    dt: forecast_dt,
                    temp_min: current.temp - 5.0,
                    temp_max: current.temp + 5.0,
                    condition_code: current.condition_code,
                    description: description.to_string(),
                    pop: 0.0,
                    wind_speed: current.wind_speed,
                    wind_deg: current.wind_deg,
                    humidity: current.humidity,
                    sunrise: None,
                    sunset: None,
                    uv_max: None,
                }
            })
            .collect()
    }

    /// Parse cloud coverage from oktas (0-8 scale)
    fn parse_cloud_coverage(oktas: Option<i32>) -> i32 {
        match oktas {
            Some(o) => ((o as f64 / 8.0) * 100.0) as i32,
            None => 0,
        }
    }

    /// Parse visibility from string (e.g., "10" or ">10")
    fn parse_visibility(vis: &Option<String>) -> Option<i32> {
        vis.as_ref().and_then(|v| {
            v.trim_start_matches('>')
                .parse::<f64>()
                .ok()
                .map(|km| (km * 1000.0) as i32)
        })
    }

    /// Parse wind direction from compass direction
    fn parse_wind_direction(dir: &Option<String>) -> i32 {
        match dir.as_deref() {
            Some("N") => 0,
            Some("NNE") => 22,
            Some("NE") => 45,
            Some("ENE") => 67,
            Some("E") => 90,
            Some("ESE") => 112,
            Some("SE") => 135,
            Some("SSE") => 157,
            Some("S") => 180,
            Some("SSW") => 202,
            Some("SW") => 225,
            Some("WSW") => 247,
            Some("W") => 270,
            Some("WNW") => 292,
            Some("NW") => 315,
            Some("NNW") => 337,
            Some("CALM") => 0,
            _ => 0,
        }
    }

    /// Map BOM icon descriptor to Garmin condition code
    fn map_icon_descriptor_to_garmin(icon_descriptor: &str, is_night: bool) -> i32 {
        let icon = icon_descriptor.to_lowercase();

        if icon.contains("storm") || icon.contains("thunder") {
            27 // Thunderstorm
        } else if icon.contains("shower") || icon.contains("rain") {
            17 // Rain
        } else if icon.contains("drizzle") {
            25 // Light rain
        } else if icon.contains("snow") {
            38 // Snow
        } else if icon.contains("fog") || icon.contains("mist") {
            47 // Fog
        } else if icon.contains("partly")
            || icon.contains("partly_cloudy")
            || icon.contains("partly cloudy")
        {
            // Check for partly cloudy BEFORE clear/sunny to avoid matching "partly_sunny"
            if is_night {
                10 // Partly cloudy (Garmin code 10)
            } else {
                13
            }
        } else if icon.contains("mostly_sunny") || icon.contains("mostly sunny") {
            if is_night {
                10 // Mostly sunny / partly cloudy
            } else {
                13
            }
        } else if icon.contains("clear") || icon.contains("sunny") {
            if is_night {
                4
            } else {
                5 // Clear / sunny
            }
        } else if icon.contains("cloudy") || icon.contains("overcast") {
            15 // Cloudy
        } else if icon.contains("wind") {
            47 // Windy
        } else {
            if is_night {
                4
            } else {
                5 // Clear / sunny
            }
        }
    }

    /// Get description from Garmin condition code
    fn garmin_condition_to_description(condition_code: i32) -> String {
        match condition_code {
            5 => "Clear".to_string(),
            6 => "Partly Cloudy".to_string(),
            7 => "Mostly Cloudy".to_string(),
            8 => "Partly Cloudy".to_string(),
            10 => "Partly Cloudy".to_string(),
            11 => "Light Rain".to_string(),
            12 => "Rain".to_string(),
            13 => "Snow".to_string(),
            14 => "Flurries".to_string(),
            15 => "Cloudy".to_string(),
            16 => "Fog".to_string(),
            17 => "Rain".to_string(),
            27 => "Thunderstorm".to_string(),
            38 => "Snow".to_string(),
            46 => "Windy".to_string(),
            47 => "Fog".to_string(),
            _ => "Clear".to_string(),
        }
    }

    /// Clear the weather cache
    pub async fn clear_cache(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
        log::debug!("BOM weather cache cleared");
    }

    /// Get cache statistics
    pub async fn cache_stats(&self) -> (usize, Vec<(i32, i32, i64)>) {
        let cache = self.cache.read().await;
        let size = cache.len();
        let mut entries = Vec::new();

        for ((lat, lon), cached) in cache.iter() {
            let age = Utc::now()
                .signed_duration_since(cached.fetched_at)
                .num_seconds();
            entries.push((*lat, *lon, age));
        }

        (size, entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_semicircles_conversion() {
        // Test Sydney coordinates
        let sydney_lat_sc = -405947981; // ~-33.87°
        let sydney_lon_sc = 1813871125; // ~151.21°

        let lat = BomWeatherService::semicircles_to_degrees(sydney_lat_sc);
        let lon = BomWeatherService::semicircles_to_degrees(sydney_lon_sc);

        assert!((lat + 33.87).abs() < 0.2);
        assert!((lon - 151.21).abs() < 1.0);
    }

    #[test]
    fn test_wind_direction_parsing() {
        assert_eq!(
            BomWeatherService::parse_wind_direction(&Some("N".to_string())),
            0
        );
        assert_eq!(
            BomWeatherService::parse_wind_direction(&Some("E".to_string())),
            90
        );
        assert_eq!(
            BomWeatherService::parse_wind_direction(&Some("S".to_string())),
            180
        );
        assert_eq!(
            BomWeatherService::parse_wind_direction(&Some("W".to_string())),
            270
        );
        assert_eq!(
            BomWeatherService::parse_wind_direction(&Some("NE".to_string())),
            45
        );
    }

    #[test]
    fn test_cloud_coverage() {
        assert_eq!(BomWeatherService::parse_cloud_coverage(Some(0)), 0);
        assert_eq!(BomWeatherService::parse_cloud_coverage(Some(4)), 50);
        assert_eq!(BomWeatherService::parse_cloud_coverage(Some(8)), 100);
        assert_eq!(BomWeatherService::parse_cloud_coverage(None), 0);
    }

    #[test]
    fn test_condition_mapping() {
        // Thunderstorm
        assert_eq!(
            BomWeatherService::map_bom_to_garmin_condition(&Some("Thunderstorm".to_string()), None),
            27
        );

        // Rain
        assert_eq!(
            BomWeatherService::map_bom_to_garmin_condition(&Some("Rain".to_string()), None),
            17
        );

        // Clear with few clouds
        assert_eq!(
            BomWeatherService::map_bom_to_garmin_condition(&Some("Clear".to_string()), Some(1)),
            5
        );

        // Cloudy
        assert_eq!(
            BomWeatherService::map_bom_to_garmin_condition(&None, Some(7)),
            15
        );
    }

    #[tokio::test]
    async fn test_service_creation() {
        let service = BomWeatherService::new(None);
        assert_eq!(service.cache_duration_secs, 600);
        assert!(!service.locations.is_empty());
    }

    #[tokio::test]
    async fn test_cache_operations() {
        let service = BomWeatherService::new(Some(600));

        let (size, entries) = service.cache_stats().await;
        assert_eq!(size, 0);
        assert_eq!(entries.len(), 0);

        service.clear_cache().await;
        let (size, _) = service.cache_stats().await;
        assert_eq!(size, 0);
    }

    #[test]
    fn test_nearest_location() {
        let service = BomWeatherService::new(None);

        // Test Sydney coordinates
        let sydney = service.find_nearest_location(-33.87, 151.21);
        assert!(sydney.is_some());
        assert_eq!(sydney.unwrap().name, "Sydney");

        // Test Melbourne coordinates
        let melbourne = service.find_nearest_location(-37.81, 144.96);
        assert!(melbourne.is_some());
        assert_eq!(melbourne.unwrap().name, "Melbourne");
    }
}
