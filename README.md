# watchd

This is a Linux service written to support Garmin watches. This is a service that can be run on a Linux phone or desktop to pair with Garmin smartwatches.

This project started as a vibe coded port of the Java [GadgetBridge](https://codeberg.org/Freeyourgadget/Gadgetbridge/) code to Rust. The hope is to improve the code quality and features in the future.

## Features

The following key features are supported

 - Pair with watch
 - Send notifications from Linux to the watch
     - Allow dismissing notifications on the watch
 - Proxy HTTP requests
     - Garmin Weather support, Radar currently does not work
 - Long running dameon application that attempts to reconnect to the watch
     - Should also replay notifications missed while attempting to reconnect

## Installation

TODO

## Running

```shell
cargo run --example notification_dbus_monitor -- AB:CD:EF:01:23:45 --enable-dedup --skip-pairing
```
