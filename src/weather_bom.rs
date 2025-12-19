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
            },
            BomLocation {
                name: "Melbourne".to_string(),
                state: "VIC".to_string(),
                lat: -37.8136,
                lon: 144.9631,
                wmo_id: "94866".to_string(), // Melbourne
                aac: "VIC_PT042".to_string(),
            },
            BomLocation {
                name: "Brisbane".to_string(),
                state: "QLD".to_string(),
                lat: -27.4698,
                lon: 153.0251,
                wmo_id: "94576".to_string(), // Brisbane
                aac: "QLD_PT001".to_string(),
            },
            BomLocation {
                name: "Perth".to_string(),
                state: "WA".to_string(),
                lat: -31.9505,
                lon: 115.8605,
                wmo_id: "94608".to_string(), // Perth Airport
                aac: "WA_PT001".to_string(),
            },
            BomLocation {
                name: "Adelaide".to_string(),
                state: "SA".to_string(),
                lat: -34.9285,
                lon: 138.6007,
                wmo_id: "94672".to_string(), // Adelaide
                aac: "SA_PT001".to_string(),
            },
            BomLocation {
                name: "Canberra".to_string(),
                state: "ACT".to_string(),
                lat: -35.2809,
                lon: 149.1300,
                wmo_id: "94926".to_string(), // Canberra Airport
                aac: "ACT_PT001".to_string(),
            },
            BomLocation {
                name: "Hobart".to_string(),
                state: "TAS".to_string(),
                lat: -42.8821,
                lon: 147.3272,
                wmo_id: "94970".to_string(), // Hobart
                aac: "TAS_PT001".to_string(),
            },
            BomLocation {
                name: "Darwin".to_string(),
                state: "NT".to_string(),
                lat: -12.4634,
                lon: 130.8456,
                wmo_id: "94120".to_string(), // Darwin Airport
                aac: "NT_PT001".to_string(),
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
            .ok_or(BomWeatherError::LocationNotFound(lat, lon))?;

        log::info!(
            "Using BOM location: {} ({}), station ID: {}",
            location.name,
            location.state,
            location.wmo_id
        );

        // Fetch current observations
        let observations = self.fetch_observations(location).await?;

        // Fetch forecast data
        let forecasts = self.fetch_forecasts(location).await?;

        // Parse weather data with forecasts
        let weather_data = self.parse_weather_data(location, observations, forecasts)?;

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

    /// Fetch forecasts from BOM
    async fn fetch_forecasts(
        &self,
        location: &BomLocation,
    ) -> Result<Vec<BomForecast>, BomWeatherError> {
        // BOM forecast JSON URL format
        let product_id = match location.state.as_str() {
            "NSW" => "IDN11060",
            "VIC" => "IDV10753",
            "QLD" => "IDQ11295",
            "WA" => "IDW14199",
            "SA" => "IDS10044",
            "TAS" => "IDT16710",
            "ACT" => "IDN11060", // ACT uses NSW products
            "NT" => "IDD10150",
            _ => "IDN11060", // Default to NSW
        };

        let url = format!(
            "http://www.bom.gov.au/fwo/{}/{}.json",
            product_id, product_id
        );

        log::debug!("Fetching BOM forecasts from: {}", url);

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(BomWeatherError::NoDataAvailable(format!(
                "HTTP {} for forecast {}",
                response.status(),
                product_id
            )));
        }

        let forecast_data: serde_json::Value = response.json().await?;

        // Parse the forecast data - BOM precis forecast structure
        // Structure: { observations: { notice: [...], forecasts: [{...}] } }
        let forecast_locations = forecast_data
            .get("observations")
            .and_then(|o| o.get("forecasts"))
            .and_then(|f| f.as_array())
            .ok_or_else(|| {
                BomWeatherError::InvalidData(
                    "Invalid forecast structure - missing observations.forecasts".to_string(),
                )
            })?;

        // Find the location entry matching our AAC
        let mut forecasts = Vec::new();
        for loc_entry in forecast_locations {
            if let Some(aac) = loc_entry.get("aac").and_then(|a| a.as_str()) {
                if aac == location.aac {
                    // Found our location - now extract forecast_period array
                    if let Some(periods) =
                        loc_entry.get("forecast_period").and_then(|f| f.as_array())
                    {
                        for (index, period) in periods.iter().enumerate() {
                            // Build BomForecast from the period data
                            let forecast = BomForecast {
                                index: index as i32,
                                product_id: loc_entry
                                    .get("product_id")
                                    .and_then(|p| p.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                aac: aac.to_string(),
                                name: loc_entry
                                    .get("name")
                                    .and_then(|n| n.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                state: loc_entry
                                    .get("state")
                                    .and_then(|s| s.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                forecast_period: period
                                    .get("forecast_period")
                                    .and_then(|f| f.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                start_time_local: period
                                    .get("start_time_local")
                                    .and_then(|s| s.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                end_time_local: period
                                    .get("end_time_local")
                                    .and_then(|e| e.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                minimum_temperature: period
                                    .get("minimum_temperature")
                                    .and_then(|t| t.as_f64()),
                                maximum_temperature: period
                                    .get("maximum_temperature")
                                    .and_then(|t| t.as_f64()),
                                precis: period
                                    .get("precis")
                                    .and_then(|p| p.as_str())
                                    .unwrap_or("Unknown")
                                    .to_string(),
                                probability_of_precipitation: period
                                    .get("probability_of_precipitation")
                                    .and_then(|p| p.as_str())
                                    .map(|s| s.to_string()),
                                icon_descriptor: period
                                    .get("icon_descriptor")
                                    .and_then(|i| i.as_str())
                                    .map(|s| s.to_string()),
                            };
                            forecasts.push(forecast);
                        }
                    }
                    break; // Found our location, no need to continue
                }
            }
        }

        if forecasts.is_empty() {
            log::warn!(
                "No forecasts found for location {}, using dummy data",
                location.name
            );
        }

        Ok(forecasts)
    }

    /// Parse BOM data into unified weather structure
    fn parse_weather_data(
        &self,
        location: &BomLocation,
        obs_data: BomObservationsData,
        forecasts: Vec<BomForecast>,
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
            condition_code: Self::map_bom_to_garmin_condition(
                &latest_obs.weather,
                latest_obs.cloud_oktas,
            ),
            description: String::new(), // Will be set below
        };

        // Generate proper description from condition code
        let description = if let Some(ref weather_text) = latest_obs.weather {
            if weather_text != "-" && !weather_text.is_empty() {
                weather_text.clone()
            } else {
                Self::garmin_condition_to_description(current.condition_code)
            }
        } else {
            Self::garmin_condition_to_description(current.condition_code)
        };

        let mut current = current;
        current.description = description.clone();

        // Parse daily forecasts from BOM data
        let daily: Vec<BomDailyWeather> = if !forecasts.is_empty() {
            forecasts
                .iter()
                .take(7) // Take up to 7 days
                .map(|f| {
                    let forecast_dt = dt + (f.index as i64 * 86400); // Add days in seconds

                    let temp_min = f.minimum_temperature.unwrap_or(15.0);
                    let temp_max = f.maximum_temperature.unwrap_or(25.0);

                    // Parse precipitation probability
                    let pop = if let Some(ref precip) = f.probability_of_precipitation {
                        // BOM format: "10%" or "Slight chance" etc
                        if let Some(pct) = precip.trim_end_matches('%').parse::<f64>().ok() {
                            pct / 100.0
                        } else if precip.contains("Slight") {
                            0.2
                        } else if precip.contains("Medium") {
                            0.5
                        } else if precip.contains("High") || precip.contains("Very high") {
                            0.8
                        } else {
                            0.0
                        }
                    } else {
                        0.0
                    };

                    // Map icon descriptor to condition code
                    let condition_code = if let Some(ref icon) = f.icon_descriptor {
                        Self::map_icon_descriptor_to_garmin(icon)
                    } else {
                        Self::map_bom_to_garmin_condition(&Some(f.precis.clone()), None)
                    };

                    BomDailyWeather {
                        dt: forecast_dt,
                        temp_min,
                        temp_max,
                        condition_code,
                        description: f.precis.clone(),
                        pop,
                        wind_speed: current.wind_speed, // Use current wind as forecast doesn't provide it
                        wind_deg: current.wind_deg,
                        humidity: current.humidity,
                        sunrise: None, // BOM doesn't provide sunrise/sunset in forecasts
                        sunset: None,
                    }
                })
                .collect()
        } else {
            // Fallback to basic forecast if no data available
            vec![BomDailyWeather {
                dt,
                temp_min: current.temp - 5.0,
                temp_max: current.temp + 5.0,
                condition_code: current.condition_code,
                description: description,
                pop: 0.0,
                wind_speed: current.wind_speed,
                wind_deg: current.wind_deg,
                humidity: current.humidity,
                sunrise: None,
                sunset: None,
            }]
        };

        Ok(BomWeatherData {
            lat: location.lat,
            lon: location.lon,
            location_name: location.name.clone(),
            current,
            daily,
        })
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
    fn map_icon_descriptor_to_garmin(icon_descriptor: &str) -> i32 {
        let icon = icon_descriptor.to_lowercase();

        if icon.contains("storm") || icon.contains("thunder") {
            27 // Thunderstorm
        } else if icon.contains("shower") || icon.contains("rain") {
            17 // Rain
        } else if icon.contains("drizzle") {
            11 // Light rain
        } else if icon.contains("snow") {
            38 // Snow
        } else if icon.contains("fog") || icon.contains("mist") {
            47 // Fog
        } else if icon.contains("clear") || icon.contains("sunny") {
            5 // Clear
        } else if icon.contains("mostly_sunny") || icon.contains("mostly sunny") {
            6 // Partly cloudy
        } else if icon.contains("partly_cloudy") || icon.contains("partly cloudy") {
            8 // Partly cloudy
        } else if icon.contains("cloudy") || icon.contains("overcast") {
            15 // Cloudy
        } else if icon.contains("wind") {
            46 // Windy
        } else {
            5 // Default to clear
        }
    }

    /// Get description from Garmin condition code
    fn garmin_condition_to_description(condition_code: i32) -> String {
        match condition_code {
            5 => "Clear".to_string(),
            6 => "Partly Cloudy".to_string(),
            7 => "Mostly Cloudy".to_string(),
            8 => "Partly Cloudy".to_string(),
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

    /// Map BOM weather conditions to Garmin icon codes
    fn map_bom_to_garmin_condition(weather: &Option<String>, cloud_oktas: Option<i32>) -> i32 {
        let condition = weather.as_deref().unwrap_or("").to_lowercase();

        // Thunderstorm
        if condition.contains("thunder") {
            return 27;
        }

        // Rain
        if condition.contains("rain")
            || condition.contains("shower")
            || condition.contains("drizzle")
        {
            if condition.contains("heavy") {
                return 17;
            }
            return 17;
        }

        // Snow
        if condition.contains("snow") {
            return 38;
        }

        // Fog/Mist
        if condition.contains("fog") || condition.contains("mist") || condition.contains("haze") {
            return 47;
        }

        // Dust/Smoke
        if condition.contains("dust") || condition.contains("smoke") {
            return 47;
        }

        // Wind
        if condition.contains("windy") {
            return 46;
        }

        // Cloud coverage based on oktas
        match cloud_oktas {
            Some(0..=1) => 5,  // Clear/Sunny
            Some(2..=4) => 8,  // Partly cloudy
            Some(5..=7) => 15, // Cloudy
            Some(8) => 15,     // Overcast
            _ => 5,            // Default to sunny
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
