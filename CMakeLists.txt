cmake_minimum_required(VERSION 3.26)

project(Ibex
    VERSION 0.1.0
    DESCRIPTION "Statically typed DSL for columnar DataFrame manipulation"
    LANGUAGES CXX
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

# Include CMake modules
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(CompilerOptions)
include(Dependencies)

if(IBEX_ENABLE_SANITIZERS)
    include(Sanitizers)
endif()

# Core library
add_subdirectory(src/core)
add_subdirectory(src/ir)
add_subdirectory(src/parser)
add_subdirectory(src/runtime)
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
        Ibex::codegen
)

# Plugin libraries
add_subdirectory(libs/csv)

# Tools
if(IBEX_BUILD_TOOLS)
    add_subdirectory(src/repl)
    add_subdirectory(tools)
endif()

# Tests
if(IBEX_BUILD_TESTS)
    enable_testing()
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
