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
option(IBEX_BUILD_ADBC "Build the bundled ADBC plugin" OFF)
option(IBEX_BUILD_KAFKA "Build the bundled Kafka plugin" OFF)
set(IBEX_PLUGIN_OUTPUT_DIR "${PROJECT_BINARY_DIR}/tools" CACHE PATH
    "Directory for bundled Ibex plugins (csv/gen_tf_data)")

# Include CMake modules
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(CompilerOptions)
include(Dependencies)

if(IBEX_ENABLE_SANITIZERS)
    include(Sanitizers)
endif()

include(Coverage)  # no-op unless -DIBEX_COVERAGE=ON

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
if(NOT WIN32)
    add_subdirectory(libs/udp)
    add_subdirectory(libs/websocket)
endif()
add_subdirectory(libs/lightbm)
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
