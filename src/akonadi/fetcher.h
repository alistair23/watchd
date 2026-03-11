// SPDX-License-Identifier: AGPL-3.0
//
// AkonadiCalendarFetcher — C++ bridge between Akonadi and Rust.
//
// Design constraints
// ------------------
// * All Akonadi / Qt calls MUST happen on the single OS thread that called
//   `akonadi_qt_init()`.  The Rust side enforces this by routing every call
//   through a dedicated `std::thread` that owns the QCoreApplication.
// * `akonadi_qt_init()` must be called exactly once, before any other
//   function in this header, from that dedicated thread.
// * `akonadi_fetch_calendar_ics()` is synchronous from the caller's
//   perspective: it drives its own nested QEventLoop via `KJob::exec()` and
//   only returns when all data has been collected or an error has occurred.
//
// Akonadi version support
// -----------------------
// The implementation supports KDE Frameworks 5 (Qt5) and KDE Frameworks 6
// (Qt6).  The correct headers and libraries are selected at compile time by
// the pkg-config / qmake detection in build.rs.

#pragma once

#include "rust/cxx.h"   // rust::Vec, rust::String  (injected by cxx-qt-build)

#include <cstdint>

namespace watchd_akonadi {

/// Initialise the Qt application object on the calling thread.
///
/// Creates a `QCoreApplication` if one does not already exist.
/// Must be called before `akonadi_is_available()` or
/// `akonadi_fetch_calendar_ics()`, and always from the same OS thread that
/// will subsequently call those functions.
void akonadi_qt_init();

/// Return `true` when the Akonadi server daemon is currently reachable.
///
/// Internally queries `Akonadi::ServerManager::isRunning()`.
/// Must be called from the Qt thread (i.e. after `akonadi_qt_init()`).
bool akonadi_is_available();

/// Fetch all calendar-typed items from every Akonadi collection that carries
/// the `text/calendar` MIME type.
///
/// Each element of the returned vector is a complete iCalendar (RFC 5545)
/// payload string for one Akonadi item.  Items that carry only a bare VEVENT
/// block (without a VCALENDAR wrapper) are returned as-is; the Rust caller
/// is responsible for wrapping them if required by its parser.
///
/// `start_ts` and `end_ts` are Unix timestamps (seconds since epoch).
/// Because the Akonadi IMAP protocol does not support server-side time-range
/// filtering for calendar items, ALL items in every matching collection are
/// fetched and the returned vector may therefore contain events outside the
/// requested window.  Rust-side filtering is applied after parsing.
///
/// Returns an empty vector (and logs to stderr) on any Akonadi error.
///
/// Must be called from the Qt thread.
rust::Vec<rust::String> akonadi_fetch_calendar_ics(
    ::int64_t start_ts,
    ::int64_t end_ts);

} // namespace watchd_akonadi