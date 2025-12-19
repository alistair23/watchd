//! Garmin Weather API Interceptor
//!
//! This module intercepts HTTP requests to Garmin's weather API endpoints
//! (api.gcs.garmin.com/weather/v2/forecast) and responds with weather data
//! from our local weather provider (BOM or OpenWeatherMap) instead of
//! proxying the request to the internet.
//!
//! This allows the watch to get weather data even when the phone doesn't
//! have internet connectivity, and allows us to use free weather providers
//! like Australia's Bureau of Meteorology.

use crate::http::{HttpRequest, HttpResponse};
use crate::weather_provider::{UnifiedWeatherData, UnifiedWeatherProvider};
use serde::Serialize;

/// Check if a URL is a Garmin weather API request that should be intercepted
pub fn is_garmin_weather_api(url: &str) -> bool {
    url.contains("api.gcs.garmin.com/weather") || url.contains("/weather/v2/forecast")
}

/// Check if a URL should be blocked
pub fn is_garmin_blocked_url(url: &str) -> bool {
    url.contains("connectapi.garmin.com/device-gateway")
        || url.contains("pns.ciq.garmin.com/tokens/v1/register")
}

/// Handle blocked Garmin request by returning an error response
pub fn handle_garmin_blocked_request(request: &HttpRequest) -> HttpResponse {
    println!("🚫 Blocking Garmin request");
    println!("   URL: {}", request.url);
    println!("   Path: {}", request.path);

    // Return HTTP 403 Forbidden
    let mut response = HttpResponse::new(403);
    response
        .headers
        .insert("Content-Type".to_string(), "text/plain".to_string());
    response.body = b"Blocked by Gadgetbridge".to_vec();

    response
}

/// Extract path from URL (everything after domain, before query string)
fn extract_path(url: &str) -> String {
    // Handle full URLs
    if let Some(idx) = url.find("://") {
        let after_protocol = &url[idx + 3..];
        if let Some(domain_end) = after_protocol.find('/') {
            let path_and_query = &after_protocol[domain_end..];
            if let Some(query_start) = path_and_query.find('?') {
                return path_and_query[..query_start].to_string();
            }
            return path_and_query.to_string();
        }
    }

    // Handle path-only URLs
    if let Some(query_start) = url.find('?') {
        return url[..query_start].to_string();
    }

    url.to_string()
}

/// Handle intercepted Garmin weather API request
pub async fn handle_garmin_weather_request(
    request: &HttpRequest,
    weather_provider: &UnifiedWeatherProvider,
) -> Result<HttpResponse, String> {
    println!("☀️  Intercepting Garmin weather API request");
    println!("   URL: {}", request.url);
    println!("   Path: {}", request.path);

    let path = extract_path(&request.url);
    println!("   Extracted path: {}", path);

    // Extract query parameters
    let lat = request
        .query
        .get("lat")
        .and_then(|s| s.parse::<i32>().ok())
        .ok_or_else(|| "Missing or invalid 'lat' parameter".to_string())?;

    let lon = request
        .query
        .get("lon")
        .and_then(|s| s.parse::<i32>().ok())
        .ok_or_else(|| "Missing or invalid 'lon' parameter".to_string())?;

    println!("   Coordinates: lat={}, lon={}", lat, lon);

    // Fetch weather data synchronously using tokio runtime
    let weather_data = weather_provider
        .fetch_weather(lat, lon)
        .await
        .map_err(|e| format!("Failed to fetch weather: {}", e))?;

    println!("   ✅ Weather data fetched successfully");
    println!("   Location: {}", weather_data.location_name);
    println!("   Current temp: {:.1}°C", weather_data.current.temp);
    println!("   Hourly forecasts: {}", weather_data.hourly.len());
    println!("   Daily forecasts: {}", weather_data.daily.len());

    // Determine which endpoint was requested based on path
    let response_json = if path.contains("/weather/v1/current")
        || path.contains("/weather/v2/current")
    {
        handle_current_weather(&request, &weather_data)?
    } else if path.contains("/weather/v1/forecast/hour")
        || path.contains("/weather/v2/forecast/hour")
    {
        handle_hourly_forecast(&request, &weather_data)?
    } else if path.contains("/weather/v1/forecast/day") || path.contains("/weather/v2/forecast/day")
    {
        handle_daily_forecast(&request, &weather_data)?
    } else {
        // Default to handling it as a general forecast request
        // The watch might request /weather/v2/forecast without a specific endpoint
        println!("   ℹ️  Unknown weather path, defaulting to daily forecast");
        handle_daily_forecast(&request, &weather_data)?
    };

    println!(
        "   📤 Sending weather response ({} bytes)",
        response_json.len()
    );

    let mut response = HttpResponse::ok();
    response
        .headers
        .insert("Content-Type".to_string(), "application/json".to_string());
    response.body = response_json.into_bytes();

    Ok(response)
}

/// Handle current weather endpoint
fn handle_current_weather(
    request: &HttpRequest,
    weather: &UnifiedWeatherData,
) -> Result<String, String> {
    let temp_unit = request
        .query
        .get("tempUnit")
        .map(|s| s.as_str())
        .unwrap_or("CELSIUS");

    let speed_unit = request
        .query
        .get("speedUnit")
        .map(|s| s.as_str())
        .unwrap_or("METERS_PER_SECOND");

    let pressure_unit = request
        .query
        .get("pressureUnit")
        .map(|s| s.as_str())
        .unwrap_or("MILLIBAR");

    let current = WeatherForecastCurrent {
        epoch_seconds: weather.current.dt,
        temperature: convert_temperature(weather.current.temp, temp_unit),
        description: weather.current.description.clone(),
        icon: weather.current.condition_code,
        feels_like_temperature: convert_temperature(weather.current.feels_like, temp_unit),
        dew_point: convert_temperature(weather.current.dew_point, temp_unit),
        relative_humidity: weather.current.humidity,
        wind: Wind {
            speed: convert_speed(weather.current.wind_speed, speed_unit),
            direction_string: get_wind_direction(weather.current.wind_deg),
            direction: weather.current.wind_deg,
        },
        location_name: weather.location_name.clone(),
        visibility: WeatherValue {
            value: weather.current.visibility.unwrap_or(10000) as f64,
            units: "METER".to_string(),
        },
        pressure: convert_pressure(weather.current.pressure as f64, pressure_unit),
        pressure_change: WeatherValue {
            value: 0.0,
            units: "INCHES_OF_MERCURY".to_string(),
        },
        cloud_coverage: weather.current.clouds,
    };

    serde_json::to_string(&current).map_err(|e| format!("JSON serialization error: {}", e))
}

/// Handle hourly forecast endpoint
fn handle_hourly_forecast(
    request: &HttpRequest,
    weather: &UnifiedWeatherData,
) -> Result<String, String> {
    let temp_unit = request
        .query
        .get("tempUnit")
        .map(|s| s.as_str())
        .unwrap_or("CELSIUS");

    let speed_unit = request
        .query
        .get("speedUnit")
        .map(|s| s.as_str())
        .unwrap_or("METERS_PER_SECOND");

    let duration = request
        .query
        .get("duration")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(12);

    let mut hourly_forecasts = Vec::new();

    for (i, hourly) in weather.hourly.iter().enumerate() {
        if i >= duration {
            break;
        }

        let forecast = WeatherForecastHour {
            epoch_seconds: hourly.dt,
            description: hourly.description.clone(),
            temp: convert_temperature(hourly.temp, temp_unit),
            precip_prob: Some((hourly.pop * 100.0) as i32),
            wind: Wind {
                speed: convert_speed(hourly.wind_speed, speed_unit),
                direction_string: get_wind_direction(hourly.wind_deg),
                direction: hourly.wind_deg,
            },
            icon: hourly.condition_code,
            dew_point: None, // TODO: Add dew point to hourly data
            uv_index: None,  // TODO: Add UV index to hourly data
            relative_humidity: hourly.humidity,
            feels_like_temperature: convert_temperature(hourly.feels_like, temp_unit),
            visibility: None,
            pressure: None,
            air_quality: None,
            cloud_cover: None,
        };

        hourly_forecasts.push(forecast);
    }

    serde_json::to_string(&hourly_forecasts).map_err(|e| format!("JSON serialization error: {}", e))
}

/// Handle daily forecast endpoint
fn handle_daily_forecast(
    request: &HttpRequest,
    weather: &UnifiedWeatherData,
) -> Result<String, String> {
    let temp_unit = request
        .query
        .get("tempUnit")
        .map(|s| s.as_str())
        .unwrap_or("CELSIUS");

    let speed_unit = request
        .query
        .get("speedUnit")
        .map(|s| s.as_str())
        .unwrap_or("KILOMETERS_PER_HOUR");

    let duration = request
        .query
        .get("duration")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(5);

    let version = if request.path.contains("/v2/") || request.url.contains("/v2/") {
        2
    } else {
        1
    };

    let mut daily_forecasts = Vec::new();

    for (i, daily) in weather.daily.iter().enumerate() {
        if i >= duration {
            break;
        }

        let day_of_week = get_day_of_week(daily.dt, version);

        let forecast = WeatherForecastDay {
            day_of_week,
            description: daily.description.clone(),
            summary: daily.description.clone(),
            high: convert_temperature(daily.temp_max, temp_unit),
            low: convert_temperature(daily.temp_min, temp_unit),
            precip_prob: Some((daily.pop * 100.0) as i32),
            icon: daily.condition_code,
            epoch_sunrise: daily.sunrise,
            epoch_sunset: daily.sunset,
            wind: Wind {
                speed: convert_speed(daily.wind_speed, speed_unit),
                direction_string: get_wind_direction(daily.wind_deg),
                direction: daily.wind_deg,
            },
            humidity: daily.humidity,
        };

        daily_forecasts.push(forecast);
    }

    serde_json::to_string(&daily_forecasts).map_err(|e| format!("JSON serialization error: {}", e))
}

// Serializable structs matching the Garmin API format

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WeatherForecastCurrent {
    epoch_seconds: i64,
    temperature: WeatherValue,
    description: String,
    icon: i32,
    feels_like_temperature: WeatherValue,
    dew_point: WeatherValue,
    relative_humidity: i32,
    wind: Wind,
    location_name: String,
    visibility: WeatherValue,
    pressure: WeatherValue,
    pressure_change: WeatherValue,
    cloud_coverage: i32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WeatherForecastDay {
    day_of_week: i32,
    description: String,
    summary: String,
    high: WeatherValue,
    low: WeatherValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    precip_prob: Option<i32>,
    icon: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    epoch_sunrise: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    epoch_sunset: Option<i64>,
    wind: Wind,
    humidity: i32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WeatherForecastHour {
    epoch_seconds: i64,
    description: String,
    temp: WeatherValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    precip_prob: Option<i32>,
    wind: Wind,
    icon: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    dew_point: Option<WeatherValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    uv_index: Option<f32>,
    relative_humidity: i32,
    feels_like_temperature: WeatherValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    visibility: Option<WeatherValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pressure: Option<WeatherValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    air_quality: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cloud_cover: Option<i32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WeatherValue {
    value: f64,
    units: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Wind {
    speed: WeatherValue,
    direction_string: String,
    direction: i32,
}

// Utility functions

fn convert_temperature(kelvin: f64, unit: &str) -> WeatherValue {
    match unit {
        "FAHRENHEIT" => {
            let celsius = kelvin - 273.15;
            let fahrenheit = celsius * 9.0 / 5.0 + 32.0;
            WeatherValue {
                value: fahrenheit,
                units: "FAHRENHEIT".to_string(),
            }
        }
        "KELVIN" => WeatherValue {
            value: kelvin,
            units: "KELVIN".to_string(),
        },
        "CELSIUS" | _ => {
            // Note: Gadgetbridge does a "wrong" conversion on purpose for compatibility
            // We follow the same pattern: kelvin - 273 instead of kelvin - 273.15
            WeatherValue {
                value: kelvin - 273.0,
                units: "CELSIUS".to_string(),
            }
        }
    }
}

fn convert_speed(mps: f64, unit: &str) -> WeatherValue {
    match unit {
        "METERS_PER_SECOND" => WeatherValue {
            value: mps,
            units: "METERS_PER_SECOND".to_string(),
        },
        "KILOMETERS_PER_HOUR" | _ => WeatherValue {
            value: mps * 3.6,
            units: "KILOMETERS_PER_HOUR".to_string(),
        },
    }
}

fn convert_pressure(millibar: f64, unit: &str) -> WeatherValue {
    match unit {
        "INCHES_OF_MERCURY" => WeatherValue {
            value: millibar * 0.02953,
            units: "INCHES_OF_MERCURY".to_string(),
        },
        "MILLIBAR" | _ => WeatherValue {
            value: millibar,
            units: "MILLIBAR".to_string(),
        },
    }
}

fn get_wind_direction(degrees: i32) -> String {
    let normalized = ((degrees % 360 + 360) % 360) as f64;
    let directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"];
    let index = ((normalized / 45.0).round() as usize) % 8;
    directions[index].to_string()
}

fn get_day_of_week(timestamp: i64, version: i32) -> i32 {
    use chrono::{Datelike, TimeZone, Utc, Weekday};

    let dt = Utc.timestamp_opt(timestamp, 0).unwrap();
    let weekday = dt.weekday();

    if version == 2 {
        // V2: 1 = Monday, 7 = Sunday
        match weekday {
            Weekday::Mon => 1,
            Weekday::Tue => 2,
            Weekday::Wed => 3,
            Weekday::Thu => 4,
            Weekday::Fri => 5,
            Weekday::Sat => 6,
            Weekday::Sun => 7,
        }
    } else {
        // V1: 1 = Sunday, 7 = Saturday
        match weekday {
            Weekday::Sun => 1,
            Weekday::Mon => 2,
            Weekday::Tue => 3,
            Weekday::Wed => 4,
            Weekday::Thu => 5,
            Weekday::Fri => 6,
            Weekday::Sat => 7,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_garmin_weather_api() {
        assert!(is_garmin_weather_api(
            "https://api.gcs.garmin.com/weather/v2/forecast"
        ));
        assert!(is_garmin_weather_api(
            "https://api.gcs.garmin.com/weather/v1/current"
        ));
        assert!(is_garmin_weather_api("/weather/v2/forecast/day"));
        assert!(!is_garmin_weather_api("https://example.com/api"));
    }

    #[test]
    fn test_is_garmin_blocked_url() {
        assert!(is_garmin_blocked_url(
            "https://connectapi.garmin.com/device-gateway/rest/cloudapi/consumer/device/XXXXXXXX"
        ));
        assert!(is_garmin_blocked_url(
            "https://connectapi.garmin.com/device-gateway/something"
        ));
        assert!(!is_garmin_blocked_url("https://api.gcs.garmin.com/weather"));
        assert!(!is_garmin_blocked_url("https://example.com/api"));
    }

    #[test]
    fn test_extract_path() {
        assert_eq!(
            extract_path("https://api.gcs.garmin.com/weather/v2/forecast?lat=123&lon=456"),
            "/weather/v2/forecast"
        );
        assert_eq!(
            extract_path("/weather/v1/current?lat=123"),
            "/weather/v1/current"
        );
    }

    #[test]
    fn test_wind_direction() {
        assert_eq!(get_wind_direction(0), "N");
        assert_eq!(get_wind_direction(45), "NE");
        assert_eq!(get_wind_direction(90), "E");
        assert_eq!(get_wind_direction(180), "S");
        assert_eq!(get_wind_direction(270), "W");
        assert_eq!(get_wind_direction(360), "N");
    }

    #[test]
    fn test_temperature_conversion() {
        let celsius = convert_temperature(273.0, "CELSIUS");
        assert_eq!(celsius.units, "CELSIUS");
        assert_eq!(celsius.value, 0.0);

        let kelvin = convert_temperature(273.0, "KELVIN");
        assert_eq!(kelvin.units, "KELVIN");
        assert_eq!(kelvin.value, 273.0);
    }

    #[test]
    fn test_speed_conversion() {
        let mps = convert_speed(10.0, "METERS_PER_SECOND");
        assert_eq!(mps.units, "METERS_PER_SECOND");
        assert_eq!(mps.value, 10.0);

        let kmph = convert_speed(10.0, "KILOMETERS_PER_HOUR");
        assert_eq!(kmph.units, "KILOMETERS_PER_HOUR");
        assert_eq!(kmph.value, 36.0);
    }

    #[test]
    fn test_day_of_week() {
        // Test with a known timestamp: 2024-01-01 00:00:00 UTC (Monday)
        let monday_timestamp = 1704067200;

        assert_eq!(get_day_of_week(monday_timestamp, 2), 1); // V2: Monday = 1
        assert_eq!(get_day_of_week(monday_timestamp, 1), 2); // V1: Monday = 2
    }
}
