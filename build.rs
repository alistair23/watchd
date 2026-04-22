// SPDX-License-Identifier: AGPL-3.0
//
// build.rs — Cargo build script for watchd.
//
// When the `akonadi` Cargo feature is enabled this script:
//
//   1. Locates qmake (via the QMAKE env-var or a PATH search for common names
//      such as qmake5 / qmake-qt5 / qmake6 / qmake).
//
//   2. Runs `qmake -query` to obtain the Qt installation paths
//      (QT_INSTALL_HEADERS, QT_INSTALL_LIBS, QT_INSTALL_ARCHDATA).
//
//   3. Reads the Akonadi qmake module file:
//        $QT_INSTALL_ARCHDATA/mkspecs/modules/qt_AkonadiCore.pri
//      and parses it to extract the library name (QT.AkonadiCore.module)
//      and the header search paths (QT.AkonadiCore.includes).
//      These files are shipped by the Akonadi development package alongside
//      the Qt mkspecs tree — no pkg-config or CMake required.
//
//   4. Uses `cxx-qt-build` to:
//        a. Run the `#[cxx::bridge]` code generator on src/akonadi/mod.rs.
//        b. Compile src/akonadi/fetcher.cpp with the Akonadi include paths.
//        c. Locate and link Qt Core (and Qt's own transitive dependencies).
//
//   5. Emits `cargo:rustc-link-lib` / `cargo:rustc-link-search` lines so
//      that Cargo links libKPim5AkonadiCore (or the equivalent) into the
//      final binary.
//
// Without the `akonadi` feature none of this runs, so the crate builds on
// any system regardless of whether Qt or KDE are installed.
//
// ── Environment knobs ────────────────────────────────────────────────────────
//
//   QMAKE              Path to the qmake executable.  If unset, build.rs
//                      searches PATH for qmake5, qmake-qt5, qmake6,
//                      qmake-qt6, and qmake, in that order.
//
//   QT_VERSION_MAJOR   Set to "5" or "6" to disambiguate when multiple
//                      qmake versions are on PATH.  Forwarded to
//                      cxx-qt-build's Qt detection as well.
//
//   AKONADI_INCLUDE_DIR
//                      Override the Akonadi include directory instead of
//                      reading it from the .pri file.  Useful when the
//                      mkspecs tree is in a non-standard location.
//
//   AKONADI_LIB        Override the Akonadi library name (default: value of
//                      QT.AkonadiCore.module from the .pri file, e.g.
//                      "KPim5AkonadiCore").
//
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "akonadi")]
use cxx_qt_build::CxxQtBuilder;

fn main() {
    // Tell Cargo when to re-run this script.
    println!("cargo:rerun-if-changed=src/akonadi/fetcher.h");
    println!("cargo:rerun-if-changed=src/akonadi/fetcher.cpp");
    println!("cargo:rerun-if-changed=src/akonadi/mod.rs");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=QMAKE");
    println!("cargo:rerun-if-env-changed=QT_VERSION_MAJOR");
    println!("cargo:rerun-if-env-changed=AKONADI_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=AKONADI_LIB");

    #[cfg(feature = "akonadi")]
    build_akonadi();
}

// =============================================================================
// Akonadi build logic
// =============================================================================

#[cfg(feature = "akonadi")]
fn build_akonadi() {
    // Detect qmake first and pin it in the process environment so that
    // CxxQtBuilder's internal Qt detection uses the exact same Qt installation
    // as our Akonadi .pri-file discovery.  Without this, cxx-qt-build may
    // find a different Qt version (e.g. Qt6 when KDE is built against Qt5),
    // causing an ABI mismatch between the compiled C++ and the Akonadi .so.
    //
    // SAFETY: build scripts are single-threaded; no other thread reads QMAKE
    // concurrently.
    let qmake = find_qmake();
    unsafe {
        std::env::set_var("QMAKE", &qmake);
    }

    let akonadi = find_akonadi();

    // Clone the include paths so we can move them into the cc_builder closure
    // while also keeping them for the diagnostic message below.
    let include_paths = akonadi.include_paths.clone();
    let link_paths = akonadi.link_paths.clone();
    let libs = akonadi.libs.clone();

    println!(
        "cargo:warning=watchd[akonadi]: lib={:?}  includes={:?}",
        libs, include_paths,
    );

    // ------------------------------------------------------------------
    // Build the CXX bridge and compile fetcher.cpp.
    //
    // CxxQtBuilder:
    //   • Discovers Qt via qmake (QMAKE env-var or PATH search).
    //   • Runs the cxx code-generator on src/akonadi/mod.rs.
    //   • Compiles all C++ (bridge glue + fetcher.cpp) into a static
    //     archive that Cargo links.
    //
    // We pass the Akonadi include paths and our C++ source file through
    // the cc_builder escape-hatch so they are compiled with the same
    // flags as the generated bridge glue.
    // ------------------------------------------------------------------
    CxxQtBuilder::new()
        // Qt Core is always required — Akonadi depends on it.
        // Qt Gui / Widgets are NOT needed; watchd is a headless daemon.
        .qt_module("Core")
        // The Rust file that contains the `#[cxx::bridge]` block.
        .file("src/akonadi/mod.rs")
        // Configure the underlying cc::Build that compiles all C++.
        .cc_builder(|cc| {
            // Hand-written Akonadi C++ implementation.
            cc.file("src/akonadi/fetcher.cpp");

            // Akonadi / KDE header search paths discovered from the .pri file.
            for path in &include_paths {
                cc.include(path);
            }

            // Add the crate root so that the include!("src/akonadi/fetcher.h")
            // directive in the cxx bridge resolves correctly.
            cc.include(".");

            // Both KF5 and KF6 require at least C++17.
            cc.flag_if_supported("-std=c++17");

            // Silence warnings that originate in Qt/KDE headers.
            cc.flag_if_supported("-Wno-deprecated-declarations");
            cc.flag_if_supported("-Wno-unused-parameter");
        })
        .build();

    // ------------------------------------------------------------------
    // Emit Akonadi linker directives.
    // (CxxQtBuilder already emits the Qt Core link directives.)
    // ------------------------------------------------------------------
    for path in &link_paths {
        println!("cargo:rustc-link-search=native={}", path.display());
    }
    for lib in &libs {
        println!("cargo:rustc-link-lib={}", lib);
    }
}

// =============================================================================
// Akonadi discovery via qmake .pri files
// =============================================================================

/// Everything build.rs needs to compile and link against Akonadi.
#[cfg(feature = "akonadi")]
#[derive(Default)]
struct AkonadiInfo {
    include_paths: Vec<std::path::PathBuf>,
    link_paths: Vec<std::path::PathBuf>,
    libs: Vec<String>,
}

/// Locate Akonadi headers and the library name.
///
/// Resolution order:
///   1. AKONADI_INCLUDE_DIR + AKONADI_LIB environment variable overrides.
///   2. Parse `$QT_INSTALL_ARCHDATA/mkspecs/modules/qt_AkonadiCore.pri`,
///      which is installed by the Akonadi development package alongside
///      the Qt mkspecs tree.
///   3. pkg-config fallback — used on distros (Alpine, Debian, Ubuntu, …)
///      where the Akonadi package does not install Qt mkspecs `.pri` files.
///   4. Direct filesystem probe — last resort for distros (e.g. Alpine)
///      that provide neither `.pri` files nor pkg-config metadata, but do
///      install headers under `/usr/include/KPim5` and libraries in `/usr/lib`.
#[cfg(feature = "akonadi")]
fn find_akonadi() -> AkonadiInfo {
    // ------------------------------------------------------------------
    // Manual override — handy for non-standard installations.
    // ------------------------------------------------------------------
    let override_inc = std::env::var("AKONADI_INCLUDE_DIR").ok();
    let override_lib = std::env::var("AKONADI_LIB").ok();

    if let (Some(inc), Some(lib)) = (override_inc, override_lib) {
        println!(
            "cargo:warning=watchd[akonadi]: using manual override \
             (AKONADI_INCLUDE_DIR={inc}, AKONADI_LIB={lib})"
        );
        return AkonadiInfo {
            include_paths: vec![std::path::PathBuf::from(inc)],
            link_paths: vec![],
            libs: vec![lib],
        };
    }

    // ------------------------------------------------------------------
    // Step 1 — find qmake.
    // ------------------------------------------------------------------
    let qmake = find_qmake();

    // ------------------------------------------------------------------
    // Step 2 — query the Qt installation paths.
    // ------------------------------------------------------------------
    let qt_vars = qmake_query(&qmake);

    let qt_headers = qt_vars
        .get("QT_INSTALL_HEADERS")
        .cloned()
        .unwrap_or_default();

    let qt_libs = qt_vars.get("QT_INSTALL_LIBS").cloned().unwrap_or_default();

    // QT_INSTALL_ARCHDATA is where the mkspecs tree lives, e.g.
    // /usr/lib/qt5  or  /usr/lib64/qt5
    let qt_archdata = qt_vars
        .get("QT_INSTALL_ARCHDATA")
        .cloned()
        .unwrap_or_else(|| {
            // Fallback: derive from prefix or libs dir.
            qt_vars
                .get("QT_INSTALL_PREFIX")
                .map(|p| {
                    let major = qt_vars
                        .get("QT_VERSION")
                        .and_then(|v| v.split('.').next().map(str::to_string))
                        .unwrap_or_else(|| "5".to_string());
                    format!("{}/lib/qt{}", p, major)
                })
                .unwrap_or_else(|| qt_libs.clone())
        });

    // ------------------------------------------------------------------
    // Step 3 — locate the mkspecs/modules directory.
    // ------------------------------------------------------------------
    let mkspecs_dir = find_mkspecs_dir(&qt_archdata, &qt_libs);

    // ------------------------------------------------------------------
    // Step 4 — recursively collect includes for AkonadiCore and every
    //          module it transitively depends on.
    // ------------------------------------------------------------------
    let mut visited = std::collections::HashSet::new();
    let (core_libs, mut include_paths) = collect_module_includes(
        "AkonadiCore",
        &mkspecs_dir,
        &qt_headers,
        &qt_libs,
        &mut visited,
    );

    if include_paths.is_empty() {
        // .pri-based detection yielded nothing.
        // Try pkg-config, then a direct filesystem probe, before giving up.
        println!(
            "cargo:warning=watchd[akonadi]: .pri detection found no include paths; \
             trying pkg-config fallback…"
        );
        if let Some(info) = try_pkg_config(&qt_libs) {
            return info;
        }

        println!(
            "cargo:warning=watchd[akonadi]: pkg-config fallback also failed; \
             trying direct filesystem probe…"
        );
        if let Some(info) = try_direct_probe(&qt_libs) {
            return info;
        }

        panic!(
            "watchd[akonadi]: no include paths found after resolving AkonadiCore \
             and its dependencies, and pkg-config also failed.\n\
             \n\
             Options:\n\
             • Install the Akonadi development package for your distro:\n\
                 Alpine        : apk add akonadi-dev\n\
                 Fedora/RHEL   : dnf install kf5-akonadi-server-devel\n\
                 Debian/Ubuntu : apt install libakonadi-dev\n\
                 Arch Linux    : pacman -S akonadi\n\
             • Or set the environment variables manually:\n\
                 AKONADI_INCLUDE_DIR=/path/to/akonadi/includes \\\n\
                 AKONADI_LIB=KPim5AkonadiCore \\\n\
                 cargo build --features akonadi"
        );
    }

    let core_module_name = core_libs.first().cloned().unwrap_or_default();
    println!(
        "cargo:warning=watchd[akonadi]: library='{}' (+{} transitive dep(s)) resolved {} include path(s)",
        core_module_name,
        core_libs.len().saturating_sub(1),
        include_paths.len()
    );

    // ------------------------------------------------------------------
    // Step 4b — resolve extra Akonadi modules that fetcher.cpp depends on.
    //
    //   AkonadiCalendar   — calendar-item serialiser and MIME-type helpers
    //   AkonadiAgentBase  — KJob / resource agent base classes
    //   AkonadiWidgets    — collection/item model utilities
    //   AkonadiXml        — XML (de)serialisation of calendar items
    //   AkonadiMime       — MIME message handling (CollectionFetchScope etc.)
    //
    // The shared `visited` set ensures each .pri is processed at most once;
    // transitive deps (e.g. KF5CoreAddons) discovered via AkonadiCore above
    // are still returned in core_libs and will be linked even though they
    // won't be visited again here.
    // ------------------------------------------------------------------
    let extra_modules = [
        "AkonadiCalendar",
        "AkonadiAgentBase",
        "AkonadiWidgets",
        "AkonadiXml",
        "AkonadiMime",
    ];

    // Start with all libs discovered transitively from AkonadiCore.
    let mut all_lib_names: Vec<String> = core_libs;

    for extra in &extra_modules {
        let (mod_libs, mod_includes) =
            collect_module_includes(extra, &mkspecs_dir, &qt_headers, &qt_libs, &mut visited);
        for path in mod_includes {
            if !include_paths.contains(&path) {
                include_paths.push(path);
            }
        }
        for lib in mod_libs {
            if !all_lib_names.contains(&lib) {
                println!(
                    "cargo:warning=watchd[akonadi]: extra module '{}' added lib='{}'",
                    extra, lib,
                );
                all_lib_names.push(lib);
            }
        }
    }

    // ------------------------------------------------------------------
    // Step 5 — find the directories that actually contain the .so files.
    //
    // On Fedora x86_64, Qt lives in /usr/lib64 (so QT_INSTALL_LIBS =
    // /usr/lib64) but KDE Frameworks are installed in /usr/lib.  We
    // therefore probe a prioritised list of candidates rather than
    // relying solely on QT_INSTALL_LIBS.
    // ------------------------------------------------------------------

    // Deduplicate link paths — all KDE libraries often live in the same dir.
    let mut link_paths: Vec<std::path::PathBuf> = Vec::new();
    let mut libs: Vec<String> = Vec::new();

    for lib_name in all_lib_names {
        let lib_path = probe_lib_dir(&lib_name, &qt_libs);
        if !link_paths.contains(&lib_path) {
            link_paths.push(lib_path);
        }
        libs.push(lib_name);
    }

    AkonadiInfo {
        include_paths,
        link_paths,
        libs,
    }
}

// =============================================================================
// pkg-config fallback
// =============================================================================

/// Try to discover all Akonadi build information via pkg-config.
///
/// Used as a fallback on distros (Alpine, Debian, Ubuntu, …) that do not
/// install Qt mkspecs `.pri` files alongside the Akonadi development headers.
///
/// Returns `None` if pkg-config is not installed or cannot find AkonadiCore
/// under any of the candidate module names.
#[cfg(feature = "akonadi")]
fn try_pkg_config(qt_libs: &str) -> Option<AkonadiInfo> {
    // ── Step 1: find which pkg-config name the distro uses for AkonadiCore ──
    let core_candidates = [
        "KPim6AkonadiCore", // Fedora, openSUSE, Arch (Qt6)
        "KPim5AkonadiCore", // Fedora, openSUSE, Arch (Qt5)
        "KF6AkonadiCore",   // older KDE Frameworks packaging (Qt6)
        "KF5AkonadiCore",   // older KDE Frameworks packaging (Qt5)
        "akonadi-core",     // some Alpine / Debian variants
        "akonadi",          // last-resort generic name
    ];

    let core_name = core_candidates.iter().find(|&&name| {
        std::process::Command::new("pkg-config")
            .args(["--exists", name])
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    })?;

    println!(
        "cargo:warning=watchd[akonadi]: pkg-config: using core module '{}'",
        core_name
    );

    // ── Step 2: probe extra modules (skip silently if absent) ────────────────
    let extra_candidates = [
        "KPim6AkonadiCalendar",
        "KPim6AkonadiAgentBase",
        "KPim6AkonadiWidgets",
        "KPim6AkonadiXml",
        "KPim6AkonadiMime",
        "KF6CoreAddons", // provides KJob::exec() (Qt6)
        "KPim5AkonadiCalendar",
        "KPim5AkonadiAgentBase",
        "KPim5AkonadiWidgets",
        "KPim5AkonadiXml",
        "KPim5AkonadiMime",
        "KF5CoreAddons", // provides KJob::exec() (Qt5)
    ];

    let mut all_modules: Vec<&str> = vec![core_name];
    for &extra in &extra_candidates {
        let found = std::process::Command::new("pkg-config")
            .args(["--exists", extra])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if found {
            println!(
                "cargo:warning=watchd[akonadi]: pkg-config: extra module '{}' found",
                extra
            );
            all_modules.push(extra);
        }
    }

    // ── Step 3: collect --cflags (include paths) ─────────────────────────────
    let cflags_out = std::process::Command::new("pkg-config")
        .arg("--cflags")
        .args(&all_modules)
        .output()
        .ok()?;

    let cflags = String::from_utf8_lossy(&cflags_out.stdout);

    let mut include_paths: Vec<std::path::PathBuf> = Vec::new();
    for flag in cflags.split_whitespace() {
        if let Some(path_str) = flag.strip_prefix("-I") {
            let path = std::path::PathBuf::from(path_str);
            if path.exists() && !include_paths.contains(&path) {
                include_paths.push(path);
            }
        }
    }

    // ── Step 4: collect --libs (library names + search paths) ────────────────
    let libs_out = std::process::Command::new("pkg-config")
        .arg("--libs")
        .args(&all_modules)
        .output()
        .ok()?;

    let libs_str = String::from_utf8_lossy(&libs_out.stdout);

    let mut lib_names: Vec<String> = Vec::new();
    let mut link_paths: Vec<std::path::PathBuf> = Vec::new();

    for flag in libs_str.split_whitespace() {
        if let Some(name) = flag.strip_prefix("-l") {
            let name = name.to_string();
            if !lib_names.contains(&name) {
                lib_names.push(name);
            }
        } else if let Some(path_str) = flag.strip_prefix("-L") {
            let path = std::path::PathBuf::from(path_str);
            if !link_paths.contains(&path) {
                link_paths.push(path);
            }
        }
    }

    // If pkg-config gave no -L flags the libraries are in the default linker
    // search path; add QT_INSTALL_LIBS as a best-effort hint.
    if link_paths.is_empty() && !qt_libs.is_empty() {
        link_paths.push(std::path::PathBuf::from(qt_libs));
    }

    if include_paths.is_empty() && lib_names.is_empty() {
        println!(
            "cargo:warning=watchd[akonadi]: pkg-config returned no usable flags for '{}'",
            core_name
        );
        return None;
    }

    println!(
        "cargo:warning=watchd[akonadi]: pkg-config: {} include(s), {} lib(s), {} search path(s)",
        include_paths.len(),
        lib_names.len(),
        link_paths.len()
    );

    Some(AkonadiInfo {
        include_paths,
        link_paths,
        libs: lib_names,
    })
}

// =============================================================================
// Direct filesystem probe fallback
// =============================================================================

/// Last-resort Akonadi discovery by walking well-known filesystem paths.
///
/// Used when neither `.pri` parsing nor `pkg-config` is available (e.g.
/// Alpine Linux, which ships neither for its Akonadi packages).
///
/// Strategy:
///   • Scan `/usr/include` and `/usr/local/include` for `KPim6`, `KF6`,
///     `KPim5`, and `KF5` subdirectories (Qt6 first) and collect every
///     existing path as a compiler include.
///   • For libraries, check the known KDE6/KDE5 Akonadi `.so` names against the
///     same candidate directories used by `probe_lib_dir`, keeping only those
///     whose unversioned symlink actually exists.
///   • Return `None` if no headers or libraries are found at all.
#[cfg(feature = "akonadi")]
fn try_direct_probe(qt_libs: &str) -> Option<AkonadiInfo> {
    // ── Headers ───────────────────────────────────────────────────────────────
    // Collect every KPim5/* and KF5/* subdirectory that exists under any of
    // the standard include roots.  We add both the framework root (e.g.
    // /usr/include/KPim5) and every immediate subdir (e.g.
    // /usr/include/KPim5/AkonadiCore) so that both `#include <Akonadi/Foo>`
    // and `#include <AkonadiCore/Foo>` style includes resolve.
    let include_roots = ["/usr/include", "/usr/local/include"];
    let kde_prefixes = ["KPim6", "KF6", "KPim5", "KF5"];

    let mut include_paths: Vec<std::path::PathBuf> = Vec::new();

    for root in &include_roots {
        for prefix in &kde_prefixes {
            let prefix_dir = std::path::PathBuf::from(root).join(prefix);
            if !prefix_dir.is_dir() {
                continue;
            }
            if !include_paths.contains(&prefix_dir) {
                include_paths.push(prefix_dir.clone());
            }
            // Add every immediate subdir (AkonadiCore, KCoreAddons, …).
            if let Ok(entries) = prefix_dir.read_dir() {
                for entry in entries.flatten() {
                    let sub = entry.path();
                    if sub.is_dir() && !include_paths.contains(&sub) {
                        include_paths.push(sub);
                    }
                }
            }
        }
    }

    if include_paths.is_empty() {
        println!(
            "cargo:warning=watchd[akonadi]: direct probe: \
             ///     no KPim6/KF6/KPim5/KF5 include directories found under /usr/include"
        );
        return None;
    }

    println!(
        "cargo:warning=watchd[akonadi]: direct probe: \
         found {} include path(s) under KPim6/KF6/KPim5/KF5",
        include_paths.len()
    );

    // ── Libraries ─────────────────────────────────────────────────────────────
    // Check each known KDE Akonadi library name against the candidate
    // directories.  Qt6 names are tried first; Qt5 names are kept as
    // fallback.  Only those whose unversioned `.so` symlink actually exists
    // are included (the linker needs the symlink, not the versioned file).
    let known_libs = [
        "KPim6AkonadiCore",
        "KPim6AkonadiCalendar",
        "KPim6AkonadiAgentBase",
        "KPim6AkonadiWidgets",
        "KPim6AkonadiXml",
        "KPim6AkonadiMime",
        "KF6CoreAddons",
        "KPim5AkonadiCore",
        "KPim5AkonadiCalendar",
        "KPim5AkonadiAgentBase",
        "KPim5AkonadiWidgets",
        "KPim5AkonadiXml",
        "KPim5AkonadiMime",
        "KF5CoreAddons",
    ];

    let lib_candidates = build_lib_candidates(qt_libs);

    let mut lib_names: Vec<String> = Vec::new();
    let mut link_paths: Vec<std::path::PathBuf> = Vec::new();

    for lib in &known_libs {
        let so = format!("lib{}.so", lib);
        if let Some(dir) = lib_candidates.iter().find(|d| d.join(&so).exists()) {
            println!(
                "cargo:warning=watchd[akonadi]: direct probe: found {} in {}",
                so,
                dir.display()
            );
            lib_names.push(lib.to_string());
            if !link_paths.contains(dir) {
                link_paths.push(dir.clone());
            }
        }
    }

    if lib_names.is_empty() {
        println!(
            "cargo:warning=watchd[akonadi]: direct probe: \
             no Akonadi .so files found — is akonadi-dev installed?"
        );
        return None;
    }

    Some(AkonadiInfo {
        include_paths,
        link_paths,
        libs: lib_names,
    })
}

/// Find the directory that contains `lib<name>.so` by probing a set of
/// well-known locations.
///
/// Uses a **two-pass** strategy:
///
///   Pass 1 — search every candidate for the **unversioned** `lib<name>.so`
///             symlink installed by `-devel` packages.  This is what the
///             linker needs for `-lName` to succeed.
///
///   Pass 2 — if no unversioned symlink was found anywhere, fall back to
///             accepting a versioned `lib<name>.so.N` file.  The linker will
///             likely still fail, but the error will be informative rather
///             than "library not found in search path".
///
/// The two-pass approach is critical on Fedora x86_64 where Qt lives in
/// `/usr/lib64` (versioned runtime files) but KDE's unversioned `-devel`
/// symlinks are in `/usr/lib`.  Merging both checks into a single loop would
/// cause the versioned Qt file to be found before the unversioned KDE symlink.
#[cfg(feature = "akonadi")]
fn probe_lib_dir(lib_name: &str, qt_libs: &str) -> std::path::PathBuf {
    let so_name = format!("lib{}.so", lib_name);
    let so_versioned = format!("lib{}.so.", lib_name); // prefix for e.g. .so.5

    let candidates = build_lib_candidates(qt_libs);

    // ── Pass 1: unversioned .so symlink (what the linker needs) ───────────
    for dir in &candidates {
        if dir.join(&so_name).exists() {
            println!(
                "cargo:warning=watchd[akonadi]: found {} in {}",
                so_name,
                dir.display()
            );
            return dir.clone();
        }
    }

    // ── Pass 2: versioned .so.N (runtime-only install, linking will fail
    //           but gives a better error location than an empty -L) ────────
    for dir in &candidates {
        if let Ok(entries) = dir.read_dir() {
            if entries
                .flatten()
                .any(|e| e.file_name().to_string_lossy().starts_with(&so_versioned))
            {
                println!(
                    "cargo:warning=watchd[akonadi]: found versioned {} in {} \
                     (no unversioned symlink — is the -devel package installed?)",
                    so_name,
                    dir.display()
                );
                return dir.clone();
            }
        }
    }

    println!(
        "cargo:warning=watchd[akonadi]: {} not found in any candidate dir; \
         falling back to QT_INSTALL_LIBS={}",
        so_name, qt_libs
    );
    std::path::PathBuf::from(qt_libs)
}

/// Build the ordered list of directories to probe for a library.
///
/// Order:
///   1. `QT_INSTALL_LIBS` from qmake (e.g. `/usr/lib64`)
///   2. Sibling `lib` dir            (e.g. `/usr/lib` when Qt is in `lib64`)
///   3. Inverse `lib64` dir          (e.g. `/usr/lib64` when Qt is in `lib`)
///   4. Hard-coded distro fallbacks
#[cfg(feature = "akonadi")]
fn build_lib_candidates(qt_libs: &str) -> Vec<std::path::PathBuf> {
    let mut candidates: Vec<std::path::PathBuf> = Vec::new();

    candidates.push(std::path::PathBuf::from(qt_libs));

    if let Some(parent) = std::path::Path::new(qt_libs).parent() {
        let sibling_lib = parent.join("lib");
        if sibling_lib != std::path::Path::new(qt_libs) {
            candidates.push(sibling_lib);
        }
        if qt_libs.ends_with("lib64") {
            candidates.push(parent.join("lib32"));
        } else if qt_libs.ends_with("lib") {
            candidates.push(parent.join("lib64"));
        }
    }

    for fallback in &[
        "/usr/lib",
        "/usr/lib64",
        "/usr/lib/x86_64-linux-gnu",
        "/usr/lib/aarch64-linux-gnu",
        "/usr/lib/arm-linux-gnueabihf",
    ] {
        let p = std::path::PathBuf::from(fallback);
        if !candidates.contains(&p) {
            candidates.push(p);
        }
    }

    candidates
}

/// Find the mkspecs/modules directory from the Qt installation.
///
/// Tries several layouts used by Fedora, Debian/Ubuntu, Arch, and openSUSE.
#[cfg(feature = "akonadi")]
fn find_mkspecs_dir(qt_archdata: &str, qt_libs: &str) -> std::path::PathBuf {
    let candidates: &[std::path::PathBuf] = &[
        // Standard — QT_INSTALL_ARCHDATA/mkspecs/modules
        [qt_archdata, "mkspecs", "modules"].iter().collect(),
        // Fedora: /usr/lib/qt5/mkspecs/modules
        [qt_libs, "qt5", "mkspecs", "modules"].iter().collect(),
        // Debian multiarch: /usr/lib/x86_64-linux-gnu/qt5/mkspecs/modules
        [qt_libs, "mkspecs", "modules"].iter().collect(),
        // Qt 6 on Fedora: /usr/lib64/qt6/mkspecs/modules
        [qt_libs, "qt6", "mkspecs", "modules"].iter().collect(),
    ];

    for path in candidates {
        // The directory must exist AND contain at least one .pri file to be
        // considered a valid mkspecs/modules directory.
        if path.exists() && path.read_dir().ok().and_then(|mut d| d.next()).is_some() {
            println!(
                "cargo:warning=watchd[akonadi]: mkspecs/modules dir: {}",
                path.display()
            );
            return path.clone();
        }
    }

    let list: Vec<String> = candidates
        .iter()
        .map(|p| format!("  • {}", p.display()))
        .collect();

    panic!(
        "\n\
        ╔══════════════════════════════════════════════════════════════╗\n\
        ║  watchd: Qt mkspecs/modules directory not found              ║\n\
        ╠══════════════════════════════════════════════════════════════╣\n\
        ║  Searched:                                                   ║\n\
        {}\n\
        ║                                                              ║\n\
        ║  Install Qt5 or Qt6 development tools for your distro:       ║\n\
        ║    Fedora/RHEL : dnf install qt5-qtbase-devel                ║\n\
        ║    Debian/Ubuntu: apt install qtbase5-dev                    ║\n\
        ║  Then install the Akonadi devel package:                     ║\n\
        ║    Fedora/RHEL : dnf install kf5-akonadi-server-devel        ║\n\
        ║    Debian/Ubuntu: apt install libakonadi-dev                 ║\n\
        ╚══════════════════════════════════════════════════════════════╝\n",
        list.join("\n")
    );
}

/// Recursively collect include paths for `module` and all modules it
/// transitively depends on, following `QT.<module>.depends` chains.
///
/// Returns `(all_library_names, deduplicated_include_paths)` where
/// `all_library_names[0]` is the library for `module` itself and the
/// remaining entries are the libraries of its transitive dependencies,
/// in discovery order.  Empty strings are never included in the vec.
///
/// # The `$$QT_MODULE_INCLUDE_BASE` problem
///
/// Qt's own modules (QtCore, QtNetwork, …) install their headers under
/// `QT_INSTALL_HEADERS` (e.g. `/usr/include/qt5`), so
/// `$$QT_MODULE_INCLUDE_BASE` == `QT_INSTALL_HEADERS` works for them.
///
/// KDE/KPim modules install their headers one level up (e.g.
/// `/usr/include/KF5/KCoreAddons`, `/usr/include/KPim5/AkonadiCore`) —
/// i.e. under `parent(QT_INSTALL_HEADERS)` == `/usr/include`.  To handle
/// both cases we expand `$$QT_MODULE_INCLUDE_BASE` to both values and keep
/// whichever paths actually exist on disk.
#[cfg(feature = "akonadi")]
fn collect_module_includes(
    module: &str,
    mkspecs_dir: &std::path::Path,
    qt_headers: &str,
    qt_libs: &str,
    visited: &mut std::collections::HashSet<String>,
) -> (Vec<String>, Vec<std::path::PathBuf>) {
    if !visited.insert(module.to_string()) {
        return (vec![], vec![]);
    }

    // ------------------------------------------------------------------
    // Locate the .pri file for this module.
    // ------------------------------------------------------------------
    let pri_path = mkspecs_dir.join(format!("qt_{}.pri", module));
    if !pri_path.exists() {
        // Module has no .pri file (e.g. a pure Qt module like "core" whose
        // headers are already on the compiler include path via cxx-qt-build).
        return (vec![], vec![]);
    }

    let content = match std::fs::read_to_string(&pri_path) {
        Ok(c) => c,
        Err(e) => {
            println!(
                "cargo:warning=watchd[akonadi]: could not read {}: {}",
                pri_path.display(),
                e
            );
            return (vec![], vec![]);
        }
    };

    let key_module = format!("QT.{}.module", module);
    let key_includes = format!("QT.{}.includes", module);
    let key_depends = format!("QT.{}.depends", module);

    let mut library_name = String::new();
    let mut raw_includes = Vec::<String>::new();
    let mut dep_modules = Vec::<String>::new();

    // ------------------------------------------------------------------
    // Parse the .pri file.
    // ------------------------------------------------------------------
    for raw_line in content.lines() {
        // Strip inline # comments.
        let line = if let Some(idx) = raw_line.find('#') {
            &raw_line[..idx]
        } else {
            raw_line
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        // Split `KEY = VALUE` or `KEY += VALUE` on the first `=`.
        let Some(eq_pos) = line.find('=') else {
            continue;
        };
        let key = line[..eq_pos].trim().trim_end_matches('+').trim();
        let value = line[eq_pos + 1..].trim();

        if key == key_module {
            library_name = value.to_string();
        } else if key == key_includes {
            for part in value.split_whitespace() {
                raw_includes.push(part.to_string());
            }
        } else if key == key_depends {
            for dep in value.split_whitespace() {
                // Capitalise the first letter to match .pri file naming
                // convention (e.g. "core" → "Core", "KCoreAddons" stays).
                let dep_name = {
                    let mut c = dep.chars();
                    match c.next() {
                        None => String::new(),
                        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
                    }
                };
                if !dep_name.is_empty() {
                    dep_modules.push(dep_name);
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Expand $$QT_MODULE_INCLUDE_BASE.
    //
    // We try two bases:
    //   • qt_headers itself        e.g. /usr/include/qt5
    //   • parent(qt_headers)       e.g. /usr/include
    //
    // KDE modules put their headers under /usr/include/{KF5,KPim5}/…,
    // which is one level above Qt's own headers dir.
    // ------------------------------------------------------------------
    let qt_headers_parent = std::path::Path::new(qt_headers)
        .parent()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|| qt_headers.to_string());

    let mut include_paths: Vec<std::path::PathBuf> = Vec::new();

    for raw in &raw_includes {
        for base in &[qt_headers, qt_headers_parent.as_str()] {
            let expanded = expand_qmake_vars(raw, base, qt_libs);
            let path = std::path::PathBuf::from(&expanded);
            if path.exists() && !include_paths.contains(&path) {
                include_paths.push(path);
                break; // first matching base wins for this entry
            }
        }
    }

    // ------------------------------------------------------------------
    // Fallback: if the .pri include list gave us nothing (e.g. the
    // expansion base was wrong), probe the conventional KDE layout.
    // ------------------------------------------------------------------
    if include_paths.is_empty() && !library_name.is_empty() {
        // Derive the KDE-style subdir from the library name, e.g.
        //   KPim5AkonadiCore → KPim5/AkonadiCore
        //   KF5KCoreAddons   → KF5/KCoreAddons
        for probe_base in &[qt_headers, qt_headers_parent.as_str(), "/usr/include"] {
            let base_path = std::path::PathBuf::from(probe_base);
            // Try every first-level subdir (KF5, KPim5, …) looking for
            // a directory that matches part of the library name.
            if let Ok(entries) = base_path.read_dir() {
                for entry in entries.flatten() {
                    let sub = entry.path();
                    if !sub.is_dir() {
                        continue;
                    }
                    let sub_name = sub.file_name().unwrap_or_default().to_string_lossy();
                    // e.g. library "KPim5AkonadiCore" contains prefix "KPim5"
                    if library_name.starts_with(sub_name.as_ref()) {
                        if !include_paths.contains(&sub) {
                            include_paths.push(sub.clone());
                        }
                        // Also look for the specific module subdir inside.
                        let suffix = &library_name[sub_name.len()..];
                        let module_sub = sub.join(suffix);
                        if module_sub.exists() && !include_paths.contains(&module_sub) {
                            include_paths.push(module_sub);
                        }
                        break;
                    }
                }
            }
        }
    }

    println!(
        "cargo:warning=watchd[akonadi]: module '{}' → lib='{}' paths={:?}",
        module, library_name, include_paths
    );

    // ------------------------------------------------------------------
    // Recurse into dependencies, collecting their library names too.
    // ------------------------------------------------------------------
    let mut all_libs: Vec<String> = Vec::new();
    if !library_name.is_empty() {
        all_libs.push(library_name);
    }

    for dep in &dep_modules {
        let (dep_libs, dep_includes) =
            collect_module_includes(dep, mkspecs_dir, qt_headers, qt_libs, visited);
        for lib in dep_libs {
            if !all_libs.contains(&lib) {
                all_libs.push(lib);
            }
        }
        for path in dep_includes {
            if !include_paths.contains(&path) {
                include_paths.push(path);
            }
        }
    }

    (all_libs, include_paths)
}

/// Expand the qmake variable references that appear in .pri include lists.
///
/// `base` is the candidate value for `$$QT_MODULE_INCLUDE_BASE`; callers
/// should try both `QT_INSTALL_HEADERS` and its parent directory because KDE
/// modules install headers one level above Qt's own headers directory.
#[cfg(feature = "akonadi")]
fn expand_qmake_vars(s: &str, base: &str, qt_libs: &str) -> String {
    s.replace("$$QT_MODULE_INCLUDE_BASE", base)
        .replace("$$(QT_MODULE_INCLUDE_BASE)", base)
        .replace("$$[QT_INSTALL_HEADERS]", base)
        .replace("$$QT_MODULE_LIB_BASE", qt_libs)
        .replace("$$(QT_MODULE_LIB_BASE)", qt_libs)
        .replace("$$[QT_INSTALL_LIBS]", qt_libs)
}

// =============================================================================
// qmake helpers
// =============================================================================

/// Return the path (or name) of the qmake executable.
///
/// Resolution order:
///   1. `QMAKE` environment variable.
///   2. Candidates filtered by `QT_VERSION_MAJOR` if set.
///   3. Ordered PATH search: qmake5, qmake-qt5, qmake6, qmake-qt6, qmake.
#[cfg(feature = "akonadi")]
fn find_qmake() -> String {
    // Explicit override.
    if let Ok(qmake) = std::env::var("QMAKE") {
        println!("cargo:warning=watchd[akonadi]: using QMAKE={}", qmake);
        return qmake;
    }

    // Narrow candidates when the caller has specified a Qt major version.
    let candidates: &[&str] = match std::env::var("QT_VERSION_MAJOR")
        .unwrap_or_default()
        .as_str()
    {
        "5" => &["qmake5", "qmake-qt5", "qmake"],
        "6" => &["qmake6", "qmake-qt6", "qmake"],
        _ => &["qmake6", "qmake-qt6", "qmake5", "qmake-qt5", "qmake"],
    };

    for &candidate in candidates {
        let probe = std::process::Command::new(candidate)
            .arg("-query")
            .arg("QT_VERSION")
            .output();

        if probe.map(|o| o.status.success()).unwrap_or(false) {
            println!(
                "cargo:warning=watchd[akonadi]: found qmake as '{}'",
                candidate
            );
            return candidate.to_string();
        }
    }

    panic!(
        "\n\
        ╔══════════════════════════════════════════════════════════════╗\n\
        ║  watchd: qmake not found                                     ║\n\
        ╠══════════════════════════════════════════════════════════════╣\n\
        ║  Install Qt6 development tools for your distro:              ║\n\
        ║    Fedora/RHEL : dnf install qt6-qtbase-devel                ║\n\
        ║    Debian/Ubuntu: apt install qt6-base-dev                   ║\n\
        ║    Arch Linux  : pacman -S qt6-base                          ║\n\
        ║    Alpine      : apk add qt6-qtbase-dev                      ║\n\
        ║    openSUSE    : zypper install qt6-base-devel               ║\n\
        ║                                                              ║\n\
        ║  Or point to your qmake directly:                            ║\n\
        ║    QMAKE=/path/to/qmake cargo build --features akonadi       ║\n\
        ╚══════════════════════════════════════════════════════════════╝\n"
    );
}

/// Run `qmake -query` and return all reported key/value pairs.
///
/// Output format is one `KEY:VALUE` pair per line.  Values may themselves
/// contain colons (e.g. `/usr/lib64`), so we split on the *first* colon only.
#[cfg(feature = "akonadi")]
fn qmake_query(qmake: &str) -> std::collections::HashMap<String, String> {
    let output = std::process::Command::new(qmake)
        .arg("-query")
        .output()
        .unwrap_or_else(|e| panic!("Failed to run '{}': {}", qmake, e));

    if !output.status.success() {
        panic!(
            "'{}' -query failed (exit {}): {}",
            qmake,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            // Split on the first ':' only; values like "/usr/lib64" are safe.
            let mut parts = line.splitn(2, ':');
            let key = parts.next()?.trim().to_string();
            let val = parts.next()?.trim().to_string();
            if key.is_empty() || val.is_empty() {
                return None;
            }
            Some((key, val))
        })
        .collect()
}
