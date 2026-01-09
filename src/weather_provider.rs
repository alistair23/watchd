//! Unified Weather Provider
//!
//! This module provides a unified interface for different weather providers.
//! Supports:
//! - OpenWeatherMap (global coverage, requires API key)
//! - Australian Bureau of Meteorology (Australia only, no API key)
//! - OpenAQ (global air quality data, requires API key)
//!
//! The provider can be selected at runtime based on location or configuration.

use crate::air_quality_openaq::{AirQualityData, OpenAqError, OpenAqService};
use crate::weather::semicircles_to_degrees;
use crate::weather_bom::{BomWeatherError, BomWeatherService};
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WeatherProviderError {
    #[error("BOM error: {0}")]
    Bom(#[from] BomWeatherError),

    #[error("OpenAQ error: {0}")]
    OpenAq(#[from] OpenAqError),

    #[error("No suitable weather provider available")]
    NoProvider,

    #[error("Provider not configured: {0}")]
    NotConfigured(String),
}

/// Weather provider type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeatherProviderType {
    /// Australian Bureau of Meteorology
    Bom,
    /// Automatically select based on location
    Auto,
}

impl WeatherProviderType {
    /// Parse provider type from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "bom" | "australia" => Some(WeatherProviderType::Bom),
            "auto" | "automatic" => Some(WeatherProviderType::Auto),
            _ => None,
        }
    }
}

/// Unified weather data (provider-agnostic)
#[derive(Debug, Clone)]
pub struct UnifiedWeatherData {
    pub lat: f64,
    pub lon: f64,
    pub location_name: String,
    pub current: UnifiedCurrentWeather,
    pub hourly: Vec<UnifiedHourlyWeather>,
    pub daily: Vec<UnifiedDailyWeather>,
}

#[derive(Debug, Clone)]
pub struct UnifiedCurrentWeather {
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
    pub air_quality: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct UnifiedHourlyWeather {
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
pub struct UnifiedDailyWeather {
    pub dt: i64,
    pub sunrise: Option<i64>,
    pub sunset: Option<i64>,
    pub temp_min: f64,
    pub temp_max: f64,
    pub humidity: i32,
    pub wind_speed: f64,
    pub wind_deg: i32,
    pub pop: f64,
    pub condition_code: i32,
    pub description: String,
    pub uv_max: Option<f32>,
}

/// Unified weather provider
pub struct UnifiedWeatherProvider {
    provider_type: WeatherProviderType,
    bom_service: Option<Arc<BomWeatherService>>,
    openaq_service: Option<Arc<OpenAqService>>,
}

impl UnifiedWeatherProvider {
    /// Create a new unified weather provider
    ///
    /// # Arguments
    /// * `provider_type` - Type of provider (BOM, or Auto)
    /// * `cache_duration` - Cache duration in seconds
    pub fn new(provider_type: WeatherProviderType, cache_duration: Option<i64>) -> Self {
        let bom_service = if provider_type == WeatherProviderType::Bom
            || provider_type == WeatherProviderType::Auto
        {
            Some(Arc::new(BomWeatherService::new(cache_duration)))
        } else {
            None
        };

        Self {
            provider_type,
            bom_service,
            openaq_service: None,
        }
    }

    /// Enable OpenAQ air quality service
    ///
    /// # Arguments
    /// * `api_key` - OpenAQ API key (get from https://openaq.org)
    /// * `cache_duration` - Cache duration in seconds
    pub fn enable_air_quality(&mut self, api_key: String, cache_duration: Option<i64>) {
        self.openaq_service = Some(Arc::new(OpenAqService::new(Some(api_key), cache_duration)));
    }

    /// Fetch air quality data for a location
    ///
    /// # Arguments
    /// * `lat_semicircles` - Latitude in Garmin semicircles
    /// * `lon_semicircles` - Longitude in Garmin semicircles
    pub async fn fetch_air_quality(
        &self,
        lat_semicircles: i32,
        lon_semicircles: i32,
    ) -> Result<AirQualityData, WeatherProviderError> {
        let lat = semicircles_to_degrees(lat_semicircles);
        let lon = semicircles_to_degrees(lon_semicircles);

        let service = self
            .openaq_service
            .as_ref()
            .ok_or(WeatherProviderError::NotConfigured("OpenAQ".to_string()))?;

        let data = service.fetch_air_quality(lat, lon, None).await?;
        Ok(data)
    }

    /// Fetch weather data with air quality enrichment
    ///
    /// This method fetches weather data and attempts to enrich it with air quality
    /// information if OpenAQ service is enabled.
    ///
    /// # Arguments
    /// * `lat_semicircles` - Latitude in Garmin semicircles
    /// * `lon_semicircles` - Longitude in Garmin semicircles
    pub async fn fetch_weather_with_air_quality(
        &self,
        lat_semicircles: i32,
        lon_semicircles: i32,
    ) -> Result<UnifiedWeatherData, WeatherProviderError> {
        // Fetch weather data
        let mut weather_data = self.fetch_weather(lat_semicircles, lon_semicircles).await?;

        // Try to fetch air quality and add AQI to current and hourly weather
        if let Ok(air_quality) = self
            .fetch_air_quality(lat_semicircles, lon_semicircles)
            .await
        {
            // Add AQI to current weather
            weather_data.current.air_quality = air_quality.garmin_aq;

            // Add AQI to all hourly forecasts
            for hourly in &mut weather_data.hourly {
                hourly.air_quality = air_quality.garmin_aq;
            }

            log::info!(
                "✅ Enriched weather data with Garmin AQ: {} from {}",
                air_quality
                    .garmin_aq
                    .map_or("N/A".to_string(), |v| v.to_string()),
                air_quality.location_name
            );
        }

        Ok(weather_data)
    }

    /// Fetch weather data using the appropriate provider
    ///
    /// # Arguments
    /// * `lat_semicircles` - Latitude in Garmin semicircles
    /// * `lon_semicircles` - Longitude in Garmin semicircles
    pub async fn fetch_weather(
        &self,
        lat_semicircles: i32,
        lon_semicircles: i32,
    ) -> Result<UnifiedWeatherData, WeatherProviderError> {
        let lat = semicircles_to_degrees(lat_semicircles);
        let lon = semicircles_to_degrees(lon_semicircles);

        match self.provider_type {
            WeatherProviderType::Bom => self.fetch_from_bom(lat_semicircles, lon_semicircles).await,
            WeatherProviderType::Auto => {
                // Auto-select based on location
                if self.is_in_australia(lat, lon) {
                    log::info!("Auto-selected BOM for Australian location");
                    match self.fetch_from_bom(lat_semicircles, lon_semicircles).await {
                        Ok(data) => Ok(data),
                        Err(e) => {
                            log::error!("BOM failed: {}", e);
                            return Err(e);
                        }
                    }
                } else {
                    log::error!("No provider for non-Australian location");
                    return Err(WeatherProviderError::NoProvider);
                }
            }
        }
    }

    /// Check if coordinates are in Australia
    fn is_in_australia(&self, lat: f64, lon: f64) -> bool {
        // Australia bounding box (approximate)
        // Lat: -44 to -10
        // Lon: 113 to 154
        lat >= -44.0 && lat <= -10.0 && lon >= 113.0 && lon <= 154.0
    }

    /// Fetch from BOM
    async fn fetch_from_bom(
        &self,
        lat_semicircles: i32,
        lon_semicircles: i32,
    ) -> Result<UnifiedWeatherData, WeatherProviderError> {
        let service = self
            .bom_service
            .as_ref()
            .ok_or(WeatherProviderError::NotConfigured("BOM".to_string()))?;

        let data = service
            .fetch_weather(lat_semicircles, lon_semicircles)
            .await?;
        Ok(Self::convert_bom_to_unified(data))
    }

    /// Convert BOM data to unified format
    /// BOM provides temperatures in Celsius, but UnifiedWeatherData expects Kelvin
    fn convert_bom_to_unified(bom: crate::weather_bom::BomWeatherData) -> UnifiedWeatherData {
        // Convert Celsius to Kelvin
        let celsius_to_kelvin = |c: f64| c + 273.15;

        UnifiedWeatherData {
            lat: bom.lat,
            lon: bom.lon,
            location_name: bom.location_name,
            current: UnifiedCurrentWeather {
                dt: bom.current.dt,
                temp: celsius_to_kelvin(bom.current.temp),
                feels_like: celsius_to_kelvin(bom.current.feels_like),
                pressure: bom.current.pressure,
                humidity: bom.current.humidity,
                dew_point: celsius_to_kelvin(bom.current.dew_point),
                clouds: bom.current.clouds,
                visibility: bom.current.visibility,
                wind_speed: bom.current.wind_speed,
                wind_deg: bom.current.wind_deg,
                condition_code: bom.current.condition_code,
                description: bom.current.description,
                air_quality: None,
            },
            hourly: bom
                .hourly
                .iter()
                .map(|h| UnifiedHourlyWeather {
                    dt: h.dt,
                    temp: celsius_to_kelvin(h.temp),
                    feels_like: celsius_to_kelvin(h.feels_like),
                    humidity: h.humidity,
                    wind_speed: h.wind_speed,
                    wind_deg: h.wind_deg,
                    pop: h.pop,
                    condition_code: h.condition_code,
                    description: h.description.clone(),
                    uv_index: h.uv_index,
                    dew_point: h.dew_point.map(celsius_to_kelvin),
                    pressure: h.pressure,
                    visibility: h.visibility,
                    clouds: h.clouds,
                    air_quality: h.air_quality,
                })
                .collect(),
            daily: bom
                .daily
                .iter()
                .map(|d| UnifiedDailyWeather {
                    dt: d.dt,
                    sunrise: d.sunrise,
                    sunset: d.sunset,
                    temp_min: celsius_to_kelvin(d.temp_min),
                    temp_max: celsius_to_kelvin(d.temp_max),
                    humidity: d.humidity,
                    wind_speed: d.wind_speed,
                    wind_deg: d.wind_deg,
                    pop: d.pop,
                    condition_code: d.condition_code,
                    description: d.description.clone(),
                    uv_max: d.uv_max,
                })
                .collect(),
        }
    }

    /// Get provider type
    pub fn provider_type(&self) -> WeatherProviderType {
        self.provider_type
    }

    /// Clear cache for all enabled providers
    pub async fn clear_cache(&self) {
        if let Some(ref service) = self.bom_service {
            service.clear_cache().await;
        }
        if let Some(ref service) = self.openaq_service {
            service.clear_cache().await;
        }
    }

    /// Get cache statistics for all enabled providers
    pub async fn cache_stats(&self) -> Vec<(String, usize, Vec<(i32, i32, i64)>)> {
        let mut stats = Vec::new();

        if let Some(ref service) = self.bom_service {
            let (size, entries) = service.cache_stats().await;
            stats.push(("BOM".to_string(), size, entries));
        }

        stats
    }

    /// Get air quality cache statistics
    pub async fn air_quality_cache_stats(&self) -> Option<(usize, Vec<(String, i64)>)> {
        if let Some(ref service) = self.openaq_service {
            return Some(service.cache_stats().await);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_type_parsing() {
        assert_eq!(
            WeatherProviderType::from_str("bom"),
            Some(WeatherProviderType::Bom)
        );
        assert_eq!(
            WeatherProviderType::from_str("australia"),
            Some(WeatherProviderType::Bom)
        );
        assert_eq!(
            WeatherProviderType::from_str("auto"),
            Some(WeatherProviderType::Auto)
        );
        assert_eq!(WeatherProviderType::from_str("invalid"), None);
    }

    #[test]
    fn test_australia_detection() {
        let provider = UnifiedWeatherProvider::new(WeatherProviderType::Auto, None);

        // Sydney
        assert!(provider.is_in_australia(-33.87, 151.21));

        // Melbourne
        assert!(provider.is_in_australia(-37.81, 144.96));

        // Perth
        assert!(provider.is_in_australia(-31.95, 115.86));

        // London (not Australia)
        assert!(!provider.is_in_australia(51.51, -0.13));

        // New York (not Australia)
        assert!(!provider.is_in_australia(40.71, -74.01));

        // Tokyo (not Australia)
        assert!(!provider.is_in_australia(35.68, 139.65));
    }

    #[tokio::test]
    async fn test_provider_creation() {
        let provider = UnifiedWeatherProvider::new(WeatherProviderType::Bom, Some(600));
        assert!(provider.bom_service.is_some());

        let provider = UnifiedWeatherProvider::new(WeatherProviderType::Auto, Some(600));
        assert!(provider.bom_service.is_some());
    }
}
