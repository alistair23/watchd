# watchd

This is a Linux service written to support Garmin watches. This is a service that can be run on a Linux phone or desktop to pair with Garmin smartwatches.

This project started as a vibe coded port of the Java [GadgetBridge](https://codeberg.org/Freeyourgadget/Gadgetbridge/) code to Rust. The hope is to improve the code quality and features in the future.

Currently most key features are supported, the next step is to improve documentation, code structure and code quality.

## Features

The following key features are supported

 - Pair with watch
 - Send notifications from Linux to the watch
     - Allow dismissing notifications on the watch
 - Proxy HTTP requests
     - Proxy HTTP requests from the watch to the internet and return the response
     - Weather updates, sources from BOM instead of Garmin for privacy
     - Air quality data from OpenAQ (requires API key)
     - Garmin Radar images
 - ConnectIQHTTP support
     - Allow custom applications (like GarminHomeAssistant) to work on the watch
 - Calendar sync support
 - Long running dameon application that attempts to reconnect to the watch
     - Should also replay notifications missed while attempting to reconnect

Note that unlike GadgetBridge all HTTP requests are sent, which includes requests to the Garmin servers.

## Air Quality Support

Air quality data is provided by [OpenAQ](https://openaq.org), a global open air quality data platform. To enable air quality features:

1. Get a free API key from https://openaq.org
2. Set the `OPENAQ_API_KEY` environment variable when running watchd

The service will automatically fetch air quality data for your current location, including:
- PM2.5 and PM10 particulate matter
- Ozone (O3)
- Nitrogen Dioxide (NO2)
- Sulfur Dioxide (SO2)
- Carbon Monoxide (CO)
- Calculated Air Quality Index (AQI)

Air quality data is fetched from nearby monitoring stations (within 10km radius) and cached for 10 minutes.

## Installation

TODO

## Running

TODO

```shell
CALENDAR_URLS="https://domain.com/mycal.ics" cargo run --example notification_dbus_monitor -- AB:CD:EF:01:23:45 --enable-dedup --enable-calendar-sync
```

With air quality support:

```shell
OPENAQ_API_KEY="your-api-key" CALENDAR_URLS="https://domain.com/mycal.ics" cargo run --example notification_dbus_monitor -- AB:CD:EF:01:23:45 --enable-dedup --enable-calendar-sync
```
