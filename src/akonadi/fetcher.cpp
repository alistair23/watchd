// SPDX-License-Identifier: AGPL-3.0
//
// AkonadiCalendarFetcher — C++ implementation.
//
// See fetcher.h for the full design rationale and threading contract.

#include "fetcher.h"

// ---------------------------------------------------------------------------
// Qt / KDE Frameworks includes
// ---------------------------------------------------------------------------

// QCoreApplication and friends
#include <QCoreApplication>
#include <QByteArray>
#include <QDateTime>
#include <QStringList>
#include <QTextStream>

// Akonadi client library — headers live under <Akonadi/…> for both KF5 and
// KF6 once the pkg-config include paths are in effect.
#include <Akonadi/CollectionFetchJob>
#include <Akonadi/CollectionFetchScope>
#include <Akonadi/Item>
#include <Akonadi/ItemFetchJob>
#include <Akonadi/ItemFetchScope>
#include <Akonadi/ServerManager>
#include <Akonadi/Session>

// ---------------------------------------------------------------------------
// C++ standard library
// ---------------------------------------------------------------------------
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

namespace {

// Storage for a QCoreApplication that we create ourselves.
// Raw pointer — QCoreApplication must outlive everything else Qt-related, so
// we intentionally never delete it (the OS reclaims resources on exit).
QCoreApplication* g_app_instance = nullptr;

// Fake argc/argv required by QCoreApplication.  These must remain valid for
// the lifetime of the application object, so they are static.
int    g_fake_argc    = 1;
char   g_fake_argv0[] = "watchd";
char*  g_fake_argv[]  = { g_fake_argv0, nullptr };

// Guard so that QCoreApplication is only ever created once even if
// akonadi_qt_init() is accidentally called more than once.
std::once_flag g_init_flag;

// ---------------------------------------------------------------------------
// Logging shim — writes to stderr so the Rust tracing/log infrastructure can
// capture it via the environment (e.g. RUST_LOG=watchd=debug).
// ---------------------------------------------------------------------------
void log_err(const char* msg)
{
    std::fprintf(stderr, "[watchd::akonadi] ERROR: %s\n", msg);
}

void log_info(const char* msg)
{
    std::fprintf(stderr, "[watchd::akonadi] INFO:  %s\n", msg);
}

void log_debug(const char* msg)
{
    std::fprintf(stderr, "[watchd::akonadi] DEBUG: %s\n", msg);
}

// ---------------------------------------------------------------------------
// ensure_vcalendar_wrapper
//
// Akonadi occasionally stores individual VEVENT blobs without the surrounding
// VCALENDAR envelope.  Wrap them so that the Rust iCalendar parser (which
// requires the envelope) can handle them uniformly.
// ---------------------------------------------------------------------------
QByteArray ensure_vcalendar_wrapper(const QByteArray& payload)
{
    QByteArray trimmed = payload.trimmed();
    if (trimmed.startsWith("BEGIN:VCALENDAR")) {
        return trimmed;
    }
    // Wrap a bare VEVENT / VTODO / VJOURNAL block.
    QByteArray wrapped;
    wrapped.reserve(trimmed.size() + 64);
    wrapped.append("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//watchd//akonadi//EN\r\n");
    wrapped.append(trimmed);
    if (!trimmed.endsWith('\n')) {
        wrapped.append("\r\n");
    }
    wrapped.append("END:VCALENDAR\r\n");
    return wrapped;
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Public API — namespace watchd_akonadi
// ---------------------------------------------------------------------------

namespace watchd_akonadi {

// ---------------------------------------------------------------------------
// akonadi_qt_init
// ---------------------------------------------------------------------------
void akonadi_qt_init()
{
    std::call_once(g_init_flag, []() {
        if (QCoreApplication::instance() != nullptr) {
            // An application object already exists (e.g. the process is
            // embedded in a larger Qt app).  Nothing to do.
            log_debug("akonadi_qt_init: QCoreApplication already exists, reusing.");
            return;
        }
        g_app_instance = new QCoreApplication(g_fake_argc, g_fake_argv);
        log_info("akonadi_qt_init: QCoreApplication created for Akonadi bridge.");
    });
}

// ---------------------------------------------------------------------------
// akonadi_is_available
// ---------------------------------------------------------------------------
bool akonadi_is_available()
{
    // ServerManager::isRunning() is safe to call without a live server — it
    // simply probes the D-Bus service name.
    bool running = Akonadi::ServerManager::isRunning();
    if (running) {
        log_debug("akonadi_is_available: Akonadi server is running.");
    } else {
        log_debug("akonadi_is_available: Akonadi server is NOT running.");
    }
    return running;
}

// ---------------------------------------------------------------------------
// akonadi_fetch_calendar_ics
// ---------------------------------------------------------------------------
rust::Vec<rust::String> akonadi_fetch_calendar_ics(
    ::int64_t /*start_ts*/,
    ::int64_t /*end_ts*/)
{
    // NOTE: start_ts / end_ts are accepted for API symmetry with the Rust
    // CalendarProvider trait, but Akonadi does not expose server-side time
    // range filtering for calendar items.  Every item in every matching
    // collection is returned; the Rust caller applies the time-window filter
    // after parsing the ICS payload.

    rust::Vec<rust::String> result;

    if (!QCoreApplication::instance()) {
        log_err("akonadi_fetch_calendar_ics called before akonadi_qt_init()! "
                "Returning empty result.");
        return result;
    }

    // ------------------------------------------------------------------
    // Create a short-lived Akonadi session.  We append a millisecond
    // timestamp to make the session identifier unique across repeated calls.
    // ------------------------------------------------------------------
    QByteArray session_id =
        "watchd-calendar-"
        + QByteArray::number(QDateTime::currentMSecsSinceEpoch());

    Akonadi::Session session(session_id);

    // ------------------------------------------------------------------
    // Step 1 — Fetch all collections recursively from the Akonadi root.
    //          CollectionFetchJob::exec() drives its own nested QEventLoop
    //          so this call blocks until the server responds.
    // ------------------------------------------------------------------
    auto* collection_job = new Akonadi::CollectionFetchJob(
        Akonadi::Collection::root(),
        Akonadi::CollectionFetchJob::Recursive,
        &session);

    // Request only the metadata we need.
    // Akonadi stores calendar items under its own internal MIME types, not
    // "text/calendar" (which is the wire format).  Filter for all three
    // calendar item kinds so that event, task and journal collections are
    // all returned.
    collection_job->fetchScope().setContentMimeTypes(
        QStringList()
            << QStringLiteral("application/x-vnd.akonadi.calendar.event")
            << QStringLiteral("application/x-vnd.akonadi.calendar.todo")
            << QStringLiteral("application/x-vnd.akonadi.calendar.journal"));
    collection_job->fetchScope().setIncludeStatistics(false);

    if (!collection_job->exec()) {
        std::string err_msg =
            "CollectionFetchJob failed: "
            + collection_job->errorString().toStdString();
        log_err(err_msg.c_str());
        return result;
    }

    const Akonadi::Collection::List collections = collection_job->collections();

    if (collections.isEmpty()) {
        log_info("akonadi_fetch_calendar_ics: no calendar collections found.");
        return result;
    }

    {
        std::string msg =
            "Found " + std::to_string(collections.size()) + " calendar collection(s).";
        log_info(msg.c_str());
    }

    // ------------------------------------------------------------------
    // Step 2 — For each calendar collection, fetch every item with its
    //          full ICS payload.
    // ------------------------------------------------------------------
    for (const Akonadi::Collection& collection : collections) {
        // Double-check that this collection actually accepts calendar items.
        // The server-side filter above should have done this already, but
        // be defensive.  Match against all three Akonadi-internal calendar
        // MIME types (event, todo, journal).
        const QStringList &collectionMimes = collection.contentMimeTypes();
        const bool isCalendarCollection =
            collectionMimes.contains(
                QStringLiteral("application/x-vnd.akonadi.calendar.event")) ||
            collectionMimes.contains(
                QStringLiteral("application/x-vnd.akonadi.calendar.todo")) ||
            collectionMimes.contains(
                QStringLiteral("application/x-vnd.akonadi.calendar.journal"));
        if (!isCalendarCollection) {
            continue;
        }

        {
            std::string msg =
                "Fetching items from collection: "
                + collection.displayName().toStdString()
                + " (id=" + std::to_string(collection.id()) + ")";
            log_debug(msg.c_str());
        }

        auto* item_job = new Akonadi::ItemFetchJob(collection, &session);

        // We need the complete payload, not just the envelope metadata.
        item_job->fetchScope().fetchFullPayload(true);

        // No need to fetch attributes — we only want the raw ICS payload.
        item_job->fetchScope().fetchAllAttributes(false);

        // Note: MIME type filtering at the item level is not available via
        // ItemFetchScope.  It is unnecessary here anyway because we already
        // restricted the collection fetch to collections whose content MIME
        // types include "text/calendar", so every item in these collections
        // is a calendar item.

        if (!item_job->exec()) {
            std::string err_msg =
                "ItemFetchJob failed for collection "
                + std::to_string(collection.id())
                + ": " + item_job->errorString().toStdString();
            log_err(err_msg.c_str());
            // Continue to the next collection rather than aborting entirely.
            continue;
        }

        const Akonadi::Item::List items = item_job->items();

        {
            std::string msg =
                "  -> " + std::to_string(items.size()) + " item(s) in collection "
                + std::to_string(collection.id());
            log_debug(msg.c_str());
        }

        for (const Akonadi::Item& item : items) {
            // payloadData() returns the raw bytes as stored by the Akonadi
            // serialiser plugin — for calendar items this is the ICS text.
            QByteArray raw = item.payloadData();

            if (raw.isEmpty()) {
                // Payload not available (e.g. item not yet cached locally).
                std::string msg =
                    "  Skipping item " + std::to_string(item.id())
                    + ": empty payload.";
                log_debug(msg.c_str());
                continue;
            }

            // Ensure the ICS payload has the VCALENDAR envelope.
            QByteArray ics = ensure_vcalendar_wrapper(raw);

            // Convert to a UTF-8 std::string and push into the Rust Vec.
            result.push_back(rust::String(ics.toStdString()));
        }
    }

    {
        std::string msg =
            "akonadi_fetch_calendar_ics: returning "
            + std::to_string(result.size()) + " ICS payload(s).";
        log_info(msg.c_str());
    }

    return result;
}

} // namespace watchd_akonadi