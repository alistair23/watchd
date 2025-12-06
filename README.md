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
     - Garmin Weather support, including Radar updates
 - ConnectIQHTTP support
     - Allow custom applications (like GarminHomeAssistant) to work on the watch
 - Calendar sync support
 - Long running dameon application that attempts to reconnect to the watch
     - Should also replay notifications missed while attempting to reconnect

## Installation

TODO

## Running

TODO

```shell
CALENDAR_URLS="https://domain.com/mycal.ics" cargo run --example notification_dbus_monitor -- AB:CD:EF:01:23:45 --enable-dedup --skip-pairing --enable-calendar-sync
```
