//! OpenWeatherMap Weather Provider (Stub)
//!
//! This is a stub module for OpenWeatherMap integration.
//! Full implementation to be added later.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WeatherError {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("Invalid API response: {0}")]
    InvalidResponse(String),

    #[error("API key not configured")]
    NoApiKey,

    #[error("Location not found")]
    LocationNotFound,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CurrentWeather {
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
    pub weather: Vec<WeatherCondition>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HourlyWeather {
    pub dt: i64,
    pub temp: f64,
    pub feels_like: f64,
    pub humidity: i32,
    pub wind_speed: f64,
    pub wind_deg: i32,
    pub pop: f64,
    pub weather: Vec<WeatherCondition>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DailyWeather {
    pub dt: i64,
    pub sunrise: Option<i64>,
    pub sunset: Option<i64>,
    pub temp: Temperature,
    pub humidity: i32,
    pub wind_speed: f64,
    pub wind_deg: i32,
    pub pop: f64,
    pub weather: Vec<WeatherCondition>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Temperature {
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WeatherCondition {
    pub id: i32,
    pub main: String,
    pub description: String,
    pub icon: String,
}

/// Map OpenWeatherMap condition code to Garmin icon code
pub fn map_to_garmin_condition(owm_code: i32) -> i32 {
    match owm_code {
        // Thunderstorm
        200..=232 => 27, // Thunderstorm
        // Drizzle
        300..=321 => 11, // Light rain
        // Rain
        500..=501 => 11, // Light rain
        502..=504 => 12, // Rain
        511 => 8,        // Freezing rain
        520..=531 => 11, // Showers
        // Snow
        600..=602 => 13, // Snow
        611..=616 => 9,  // Sleet
        620..=622 => 14, // Flurries
        // Atmosphere (mist, fog, etc)
        701..=781 => 16, // Fog
        // Clear
        800 => 5, // Clear
        // Clouds
        801 => 6,  // Partly cloudy
        802 => 7,  // Mostly cloudy
        803 => 15, // Cloudy
        804 => 15, // Overcast
        _ => 1,    // Unknown/default
    }
}

/// Get wind direction string from degrees
pub fn get_wind_direction(degrees: i32) -> String {
    let normalized = ((degrees % 360 + 360) % 360) as f64;
    let directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"];
    let index = ((normalized / 45.0).round() as usize) % 8;
    directions[index].to_string()
}

/// Convert Kelvin to Celsius
pub fn kelvin_to_celsius(kelvin: f64) -> f64 {
    kelvin - 273.15
}

/// Convert semicircles to degrees
pub fn semicircles_to_degrees(semicircles: i32) -> f64 {
    (semicircles as f64) * (180.0 / 2147483648.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_semicircles_conversion() {
        let sydney_lat_sc = -405947981;
        let lat = semicircles_to_degrees(sydney_lat_sc);
        assert!((lat + 33.87).abs() < 0.2);
    }

    #[test]
    fn test_garmin_condition_mapping() {
        assert_eq!(map_to_garmin_condition(800), 5); // Clear
        assert_eq!(map_to_garmin_condition(801), 6); // Partly cloudy
        assert_eq!(map_to_garmin_condition(200), 27); // Thunderstorm
        assert_eq!(map_to_garmin_condition(500), 11); // Light rain
    }

    #[test]
    fn test_wind_direction() {
        assert_eq!(get_wind_direction(0), "N");
        assert_eq!(get_wind_direction(90), "E");
        assert_eq!(get_wind_direction(180), "S");
        assert_eq!(get_wind_direction(270), "W");
    }
}
