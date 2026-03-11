// SPDX-License-Identifier: AGPL-3.0
//
// src/akonadi/mod.rs — CXX bridge between Rust and the C++ Akonadi fetcher.
//
// Threading contract
// ------------------
// Every function in the `ffi` module MUST be called from the single dedicated
// OS thread that was used to call `akonadi_qt_init()`.  The public Rust API
// (`AkonadiHandle`) in `akonadi_calendar_provider.rs` enforces this by routing
// all calls through a long-lived `std::thread` that owns the Qt / Akonadi
// objects.  Never call the raw `ffi` functions directly from async Tokio tasks
// or from the Tokio thread pool.
//
// Generated C++ glue
// ------------------
// `cxx-qt-build` (invoked from build.rs when the `akonadi` Cargo feature is
// active) reads this file, runs the `#[cxx::bridge]` proc-macro, and emits a
// corresponding `.cc` translation unit that is compiled into the final binary
// alongside `src/akonadi/fetcher.cpp`.
//
// Call-site usage (from akonadi_calendar_provider.rs)
// ----------------------------------------------------
//   use crate::akonadi::ffi;
//   unsafe { ffi::akonadi_qt_init() };
//   let ok = unsafe { ffi::akonadi_is_available() };
//   let ics = unsafe { ffi::akonadi_fetch_calendar_ics(start, end) };

/// CXX-generated FFI bindings to the Akonadi C++ bridge.
///
/// All functions are declared in `unsafe extern "C++"` because:
///   1. They mutate global Qt state (`QCoreApplication`, Akonadi sessions).
///   2. The C++ implementations are trusted but not verified by CXX's type
///      system beyond the signature check.
///
/// The `#[cxx::bridge]` attribute macro transforms this module declaration:
/// it generates a parallel C++ translation unit (compiled by `cxx-qt-build`)
/// that verifies the declared signatures match the actual C++ definitions in
/// `fetcher.h` / `fetcher.cpp`.  Any mismatch is a **compile-time** error.
#[cxx::bridge(namespace = "watchd_akonadi")]
pub mod ffi {
    unsafe extern "C++" {
        // ------------------------------------------------------------------
        // Tell CXX where to find the C++ declarations.
        //
        // The path is resolved relative to the crate root (the directory
        // that contains Cargo.toml).  `cxx-qt-build` automatically adds the
        // crate root to the compiler's include search path, so this resolves
        // to `$CARGO_MANIFEST_DIR/src/akonadi/fetcher.h`.
        // ------------------------------------------------------------------
        include!("src/akonadi/fetcher.h");

        /// Initialise `QCoreApplication` on the calling thread.
        ///
        /// Creates a `QCoreApplication` if one does not already exist.
        /// Must be called **exactly once**, before any other function in this
        /// module, from the dedicated Qt OS thread.
        ///
        /// # Safety
        /// Must be called from the single OS thread that will subsequently
        /// own all Qt / Akonadi objects for the lifetime of the process.
        fn akonadi_qt_init();

        /// Returns `true` when the Akonadi server daemon is reachable.
        ///
        /// Internally calls `Akonadi::ServerManager::isRunning()` over D-Bus.
        ///
        /// # Safety
        /// Must be called from the Qt thread (i.e. after `akonadi_qt_init()`).
        fn akonadi_is_available() -> bool;

        /// Fetch all calendar items from every Akonadi collection whose MIME
        /// type includes `text/calendar`, and return their raw ICS payloads.
        ///
        /// Each element of the returned `Vec<String>` is one complete
        /// iCalendar document (RFC 5545).  Items stored without a `VCALENDAR`
        /// envelope are wrapped by the C++ layer before being returned, so
        /// every string in the result begins with `BEGIN:VCALENDAR`.
        ///
        /// `start_ts` and `end_ts` are Unix timestamps (seconds since the
        /// epoch).  They are forwarded to the C++ layer for API symmetry with
        /// the `CalendarProvider` trait, but Akonadi does not support
        /// server-side time-range filtering for calendar items.  All items in
        /// every matching collection are therefore returned, and time-window
        /// filtering is applied on the Rust side after parsing.
        ///
        /// Returns an empty `Vec` and logs to `stderr` on any Akonadi error.
        ///
        /// # Safety
        /// Must be called from the Qt thread (i.e. after `akonadi_qt_init()`).
        fn akonadi_fetch_calendar_ics(start_ts: i64, end_ts: i64) -> Vec<String>;
    }
}
