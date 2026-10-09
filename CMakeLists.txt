# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

cmake_minimum_required(VERSION 3.26)

if(NOT DEFINED CMAKE_C_COMPILER AND DEFINED CMAKE_CXX_COMPILER)
    if(CMAKE_CXX_COMPILER MATCHES "clang\\+\\+$")
        string(REGEX REPLACE "clang\\+\\+$" "clang" CMAKE_C_COMPILER "${CMAKE_CXX_COMPILER}")
        set(CMAKE_C_COMPILER "${CMAKE_C_COMPILER}" CACHE FILEPATH
            "C compiler matched to the configured C++ compiler" FORCE)
    endif()
endif()

project(Ibex
    VERSION 0.1.0
    DESCRIPTION "Statically typed DSL for columnar DataFrame manipulation"
    LANGUAGES C CXX
)

include(GNUInstallDirs)

# Prevent in-source builds
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
    message(FATAL_ERROR "In-source builds are not allowed. Use a separate build directory.")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Wire up ccache when available so that clean builds (fresh build dirs, branch
# switches, `git clean`) reuse object files from prior builds instead of
# recompiling from scratch. This must run before any target is declared
# (including the FetchContent'd dependencies pulled in below) so their TUs are
# cached too. Only set the launcher when the user hasn't already picked one
# (e.g. via -DCMAKE_CXX_COMPILER_LAUNCHER=... or a toolchain file).
option(IBEX_USE_CCACHE "Use ccache to speed up recompiles, if found" ON)
if(IBEX_USE_CCACHE AND NOT CMAKE_C_COMPILER_LAUNCHER AND NOT CMAKE_CXX_COMPILER_LAUNCHER)
    find_program(CCACHE_PROGRAM ccache)
    if(CCACHE_PROGRAM)
        message(STATUS "Ibex: using ccache (${CCACHE_PROGRAM})")
        set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}" CACHE STRING "" FORCE)
        set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}" CACHE STRING "" FORCE)
    endif()
endif()

# Project options
option(IBEX_WARNINGS_AS_ERRORS "Treat warnings as errors" OFF)
option(IBEX_ENABLE_LTO "Enable link-time optimization in Release" OFF)
option(IBEX_ENABLE_MARCH_NATIVE "Enable -march=native in Release" OFF)
option(IBEX_ENABLE_SANITIZERS "Enable ASan + UBSan in Debug" OFF)
option(IBEX_BUILD_TESTS "Build tests" ON)
option(IBEX_BUILD_TOOLS "Build REPL and other tools" ON)
option(IBEX_BUILD_UI "Build the bundled local browser UI" ON)
option(IBEX_BUILD_EXAMPLES "Build examples" ON)
option(IBEX_BUILD_PYTHON_BRIDGE "Build the experimental pyarrow-backed Python bridge" ON)
option(IBEX_BUILD_PARQUET "Build the bundled Parquet plugin" ON)
# The LightGBM plugin fetches and builds Microsoft's LightGBM, which is slow and
# emits warnings we don't control. It is a frozen, self-contained plugin with no
# in-tree dependents (the model tests use a mock), so CI turns it off.
option(IBEX_BUILD_LIGHTGBM "Build the bundled LightGBM plugin" ON)
option(IBEX_BUILD_ADBC "Build the bundled ADBC plugin" OFF)
option(IBEX_BUILD_KAFKA "Build the bundled Kafka plugin" OFF)
option(IBEX_KAFKA_VENDOR_AVROCPP "Vendor Apache Avro C++ for Kafka schema-registry decoding" ON)
option(IBEX_MINIMAL_RUNTIME "Build only the runtime libraries needed by language bindings" OFF)

# WebAssembly (Emscripten) build: the interpreter compiled to WASM so the
# browser UI can run queries with no local server. A spike — see wasm/README.md.
# Detected automatically from the toolchain; forces off everything that assumes
# a native host (tools, tests, sockets, threads-by-default, heavyweight plugins).
if(EMSCRIPTEN)
    set(IBEX_WASM ON)
    set(IBEX_BUILD_TESTS OFF)
    set(IBEX_BUILD_TOOLS OFF)
    set(IBEX_BUILD_UI OFF)
    set(IBEX_BUILD_EXAMPLES OFF)
    set(IBEX_BUILD_PYTHON_BRIDGE OFF)
    set(IBEX_BUILD_PARQUET OFF)
    set(IBEX_BUILD_LIGHTGBM OFF)
endif()
set(IBEX_PLUGIN_OUTPUT_DIR "${PROJECT_BINARY_DIR}/tools" CACHE PATH
    "Directory for bundled Ibex plugins")

# Multi-config generators (Visual Studio, Xcode) silently append a per-config
# subdirectory (Release/, Debug/, ...) to a target's LIBRARY/RUNTIME output
# directory unless the per-config variant of that property is *also* set.
# Every plugin (and ibex_repl_bin) sets only the plain property to
# IBEX_PLUGIN_OUTPUT_DIR, so on single-config generators (Ninja, Makefiles —
# what Linux/macOS use) everything lands directly in tools/, but on Windows'
# default Visual Studio generator plugin DLLs land in tools/Release/ instead.
# The release packaging step only looks directly in tools/ (maxdepth 1), so
# plugin DLLs silently went missing from Windows release bundles and
# `import "csv"` etc. failed to find them at runtime. Pre-seeding the
# per-config variables here (before any target is created) makes every
# config resolve to the same flat directory, matching single-config output.
foreach(_ibex_cfg_type ${CMAKE_CONFIGURATION_TYPES})
    string(TOUPPER "${_ibex_cfg_type}" _ibex_cfg_type_upper)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${_ibex_cfg_type_upper} "${IBEX_PLUGIN_OUTPUT_DIR}")
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${_ibex_cfg_type_upper} "${IBEX_PLUGIN_OUTPUT_DIR}")
endforeach()
unset(_ibex_cfg_type)
unset(_ibex_cfg_type_upper)

# Include CMake modules
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(CompilerOptions)
include(Dependencies)

# Apple Clang currently ships libc++ without the C++20 time-zone database,
# even when compiling as C++23.  Probe the library rather than using compiler
# versions: the runtime has a POSIX tzdata fallback when this API is absent.
include(CheckCXXSourceCompiles)
# The probe must compile as C++23, and it must ask for it in a way every
# toolchain understands: `-std=c++23` in CMAKE_REQUIRED_FLAGS is the GCC/Clang
# spelling that MSVC ignores, and `CMAKE_REQUIRED_STANDARD` is not a CMake
# variable at all, so setting it probes the default language mode and quietly
# answers "no" everywhere. `CMAKE_CXX_STANDARD` is the portable lever:
# CMP0067 (NEW at our 3.26 minimum) makes check_cxx_source_compiles honor it,
# and CMake picks each compiler's own flag. We set it here rather than reading
# it back because the standard is carried by `target_compile_features` on the
# ibex_compiler_options interface target, so there is no global to inherit.
#
# Getting this wrong is not visible at configure time: it disables the chrono
# path and silently selects the POSIX tzdata fallback, and a stale cache in an
# existing build tree keeps answering the old way. When changing it, verify
# against a *fresh* build directory.
set(_ibex_saved_cxx_standard "${CMAKE_CXX_STANDARD}")
set(_ibex_saved_cxx_standard_required "${CMAKE_CXX_STANDARD_REQUIRED}")
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
check_cxx_source_compiles(
    "
    #include <chrono>
    int main() { (void)std::chrono::locate_zone(\"UTC\"); }
    "
    IBEX_HAS_STD_CHRONO_TIME_ZONES)
check_cxx_source_compiles(
    "
    #include <format>
    int main() {
        (void)std::format(\"{:04d} {:.3f} {:-<{}}\", 7, 1.25, \"\", 4);
    }
    "
    IBEX_HAS_STD_FORMAT)
# Restore by unsetting when there was nothing to restore: an empty-but-defined
# CMAKE_CXX_STANDARD is not the same as an unset one.
if(_ibex_saved_cxx_standard STREQUAL "")
    unset(CMAKE_CXX_STANDARD)
else()
    set(CMAKE_CXX_STANDARD "${_ibex_saved_cxx_standard}")
endif()
if(_ibex_saved_cxx_standard_required STREQUAL "")
    unset(CMAKE_CXX_STANDARD_REQUIRED)
else()
    set(CMAKE_CXX_STANDARD_REQUIRED "${_ibex_saved_cxx_standard_required}")
endif()
unset(_ibex_saved_cxx_standard)
unset(_ibex_saved_cxx_standard_required)
if(NOT IBEX_HAS_STD_FORMAT)
    message(FATAL_ERROR
        "Ibex requires C++23 <format>. Install a newer standard library "
        "(for macOS, Xcode 15.3 or later).")
endif()
if(IBEX_HAS_STD_CHRONO_TIME_ZONES)
    add_compile_definitions(IBEX_HAS_STD_CHRONO_TIME_ZONES=1)
endif()

if(IBEX_ENABLE_SANITIZERS)
    include(Sanitizers)
endif()

include(Coverage)  # no-op unless -DIBEX_COVERAGE=ON

# Target architecture detection. Set here at the top level (rather than in a
# single subdirectory) so every subdirectory — src/runtime, tools, tests —
# sees the same value. Plain set() is directory-scoped and does NOT propagate
# to sibling subdirectories, which previously left IBEX_TARGETS_X86 undefined
# in tools/ and tests/ and silently dropped -mavx2 from the benchmarks.
set(IBEX_TARGETS_X86 OFF)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64|amd64|i[3-6]86)$")
    set(IBEX_TARGETS_X86 ON)
endif()

# Core library
add_subdirectory(src/core)
add_subdirectory(src/ir)
add_subdirectory(src/parser)
add_subdirectory(src/runtime)
add_subdirectory(src/interop)
if(NOT IBEX_MINIMAL_RUNTIME AND NOT IBEX_WASM)
    add_subdirectory(src/codegen)
endif()

# Umbrella interface library
add_library(ibex_lib INTERFACE)
add_library(Ibex::lib ALIAS ibex_lib)

target_link_libraries(ibex_lib
    INTERFACE
        Ibex::core
        Ibex::ir
        Ibex::parser
        Ibex::runtime
        Ibex::interop
)
if(NOT IBEX_MINIMAL_RUNTIME AND NOT IBEX_WASM)
    target_link_libraries(ibex_lib INTERFACE Ibex::codegen)
endif()

# Plugin libraries
if(IBEX_WASM)
    # The WASM spike statically links only data_gen (see wasm/); it builds the
    # plugin's translation unit directly rather than as a shared object.
elseif(NOT IBEX_MINIMAL_RUNTIME)
    add_subdirectory(libs/args)
    add_subdirectory(libs/csv)
    add_subdirectory(libs/fs)
    add_subdirectory(libs/json)
    add_subdirectory(libs/data_gen)
    if(NOT WIN32)
        add_subdirectory(libs/udp)
        add_subdirectory(libs/websocket)
    endif()
    if(IBEX_BUILD_LIGHTGBM)
        add_subdirectory(libs/lightgbm)
    endif()
    add_subdirectory(libs/kmeans)
    add_subdirectory(libs/pca)
    if(IBEX_BUILD_PARQUET)
        add_subdirectory(libs/parquet)
    endif()
    if(IBEX_BUILD_ADBC)
        add_subdirectory(libs/adbc)
    endif()
    if(IBEX_BUILD_KAFKA)
        add_subdirectory(libs/kafka)
    endif()
endif()

if(IBEX_WASM)
    add_subdirectory(wasm)
endif()

# Tools
if(IBEX_WASM)
    # The WASM bridge links the REPL session facade but none of the CLI tooling.
    add_subdirectory(src/repl)
elseif(IBEX_BUILD_TOOLS)
    add_subdirectory(src/repl)
    if(IBEX_BUILD_UI)
        add_subdirectory(src/ui)
    endif()
    add_subdirectory(tools)
elseif(IBEX_MINIMAL_RUNTIME)
    # The language bindings (r/ibex) evaluate on the REPL session facade.
    add_subdirectory(src/repl)
endif()

# Tests must be enabled before optional subdirectories that register tests.
if(IBEX_BUILD_TESTS)
    enable_testing()
endif()

# Python bridge
if(IBEX_BUILD_PYTHON_BRIDGE)
    add_subdirectory(python)
endif()

# Tests
if(IBEX_BUILD_TESTS)
    add_subdirectory(tests)
endif()

# Examples
if(IBEX_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# Install rules
install(
    DIRECTORY include/ibex
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

# ── Release bundle: the `runtime` install component ──────────────────────────
# `cmake --install <build> --component runtime --prefix <dir>` installs exactly
# what a user runs, and nothing a developer builds against:
#
#   bin/ibex[.exe]       the REPL / script runner / `ibex ui` server
#   bin/ui/              the bundled browser UI (src/ui)
#   lib/ibex/            plugins, their *.ibex stubs, Arrow/Parquet libraries
#
# Binaries find their libraries relative to themselves ($ORIGIN /
# @executable_path), and `ibex` searches ../lib/ibex for plugins by default
# (tools/exe_path.hpp), so the tree runs from wherever it is unpacked with no
# environment variable. Windows has no RPATH: the loader searches the exe's own
# directory, so there everything goes in bin/ beside ibex.exe, which is also
# the plugin directory `ibex` falls back to.
#
# Headers, static libraries, ibex_compile/ibex_eval and the benchmark tools stay
# in the default (Unspecified) component: a plain `cmake --install` is
# unchanged.
if(WIN32)
    set(IBEX_INSTALL_PLUGINDIR "${CMAKE_INSTALL_BINDIR}")
else()
    set(IBEX_INSTALL_PLUGINDIR "${CMAKE_INSTALL_LIBDIR}/ibex")
endif()
if(APPLE)
    set(_ibex_exe_rpath "@executable_path/../${CMAKE_INSTALL_LIBDIR}/ibex")
    set(_ibex_plugin_rpath "@loader_path")
else()
    set(_ibex_exe_rpath "$ORIGIN/../${CMAKE_INSTALL_LIBDIR}/ibex")
    set(_ibex_plugin_rpath "$ORIGIN")
endif()

if(TARGET ibex_repl_bin)
    set_target_properties(ibex_repl_bin PROPERTIES INSTALL_RPATH "${_ibex_exe_rpath}")
    install(TARGETS ibex_repl_bin
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT runtime
    )
endif()

foreach(_ibex_plugin args csv fs json data_gen udp websocket lightgbm kmeans pca
        parquet adbc kafka)
    set(_ibex_plugin_target "ibex_${_ibex_plugin}_plugin")
    if(NOT TARGET ${_ibex_plugin_target})
        continue()
    endif()
    set_target_properties(${_ibex_plugin_target} PROPERTIES
        INSTALL_RPATH "${_ibex_plugin_rpath}")
    install(TARGETS ${_ibex_plugin_target}
        LIBRARY DESTINATION "${IBEX_INSTALL_PLUGINDIR}" COMPONENT runtime
        RUNTIME DESTINATION "${IBEX_INSTALL_PLUGINDIR}" COMPONENT runtime
        # A DLL's import library is for linking against it, which nothing does.
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT devel
    )
    # Every plugin's build copies its stub to <output dir>/<name>.ibex.
    install(FILES "${IBEX_PLUGIN_OUTPUT_DIR}/${_ibex_plugin}.ibex"
        DESTINATION "${IBEX_INSTALL_PLUGINDIR}" COMPONENT runtime
    )
endforeach()

# Arrow/Parquet (libs/parquet, via FetchContent) are linked by `ibex` itself
# and by the parquet plugin. Arrow's own install rules put them, with headers,
# in the default component; this installs only the shared libraries, by SONAME,
# beside the plugins. libs/parquet sets ARROW_RPATH_ORIGIN and
# ARROW_INSTALL_NAME_RPATH so they resolve one another from there.
foreach(_ibex_arrow_target arrow_shared parquet_shared arrow_ipc_shared)
    if(NOT TARGET ${_ibex_arrow_target})
        continue()
    endif()
    get_target_property(_ibex_arrow_type ${_ibex_arrow_target} TYPE)
    if(NOT _ibex_arrow_type STREQUAL "SHARED_LIBRARY")
        continue()
    endif()
    install(TARGETS ${_ibex_arrow_target}
        LIBRARY DESTINATION "${IBEX_INSTALL_PLUGINDIR}" COMPONENT runtime NAMELINK_SKIP
        RUNTIME DESTINATION "${IBEX_INSTALL_PLUGINDIR}" COMPONENT runtime
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT devel
    )
endforeach()
# The MSVC runtime (vcruntime/msvcp DLLs) beside ibex.exe, so the Windows bundle
# starts on a machine without the Visual C++ Redistributable.
if(MSVC)
    set(CMAKE_INSTALL_SYSTEM_RUNTIME_DESTINATION "${CMAKE_INSTALL_BINDIR}")
    set(CMAKE_INSTALL_SYSTEM_RUNTIME_COMPONENT runtime)
    include(InstallRequiredSystemLibraries)
endif()

unset(_ibex_plugin)
unset(_ibex_plugin_target)
unset(_ibex_arrow_target)
unset(_ibex_arrow_type)
unset(_ibex_exe_rpath)
unset(_ibex_plugin_rpath)
