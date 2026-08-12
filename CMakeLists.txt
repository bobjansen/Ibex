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
    #include <cstdio>
    #include <format>
    #include <print>
    int main() {
        auto text = std::format(\"{:04d} {:.3f} {:-<{}}\", 7, 1.25, \"\", 4);
        std::print(\"{}\", text);
        std::print(stderr, \"{}\", text);
    }
    "
    IBEX_HAS_STD_FORMAT_AND_PRINT)
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
if(NOT IBEX_HAS_STD_FORMAT_AND_PRINT)
    message(FATAL_ERROR
        "Ibex requires C++23 <format> and <print>. Install a newer standard library "
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
add_subdirectory(src/codegen)

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
        Ibex::codegen
)

# Plugin libraries
add_subdirectory(libs/csv)
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

# Tools
if(IBEX_BUILD_TOOLS)
    add_subdirectory(src/repl)
    add_subdirectory(tools)
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
include(GNUInstallDirs)

install(
    DIRECTORY include/ibex
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)
