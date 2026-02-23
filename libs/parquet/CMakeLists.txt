cmake_minimum_required(VERSION 3.26)

project(ibex_parquet_plugin LANGUAGES CXX)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "" FORCE)
endif()

if(NOT CMAKE_INSTALL_PREFIX)
    set(CMAKE_INSTALL_PREFIX "${CMAKE_BINARY_DIR}/_install" CACHE PATH "" FORCE)
endif()

include(FetchContent)

# Config inputs
if(NOT DEFINED IBEX_ROOT)
    set(IBEX_ROOT "" CACHE PATH "Ibex repository root")
endif()
if(NOT DEFINED IBEX_BUILD_DIR)
    set(IBEX_BUILD_DIR "" CACHE PATH "Ibex build directory (for static libs)")
endif()

if(IBEX_ROOT STREQUAL "" AND DEFINED ENV{IBEX_ROOT})
    set(IBEX_ROOT "$ENV{IBEX_ROOT}" CACHE PATH "" FORCE)
endif()
if(IBEX_BUILD_DIR STREQUAL "" AND DEFINED ENV{IBEX_BUILD_DIR})
    set(IBEX_BUILD_DIR "$ENV{IBEX_BUILD_DIR}" CACHE PATH "" FORCE)
endif()

if(IBEX_ROOT STREQUAL "")
    message(FATAL_ERROR "IBEX_ROOT is required")
endif()
if(IBEX_BUILD_DIR STREQUAL "")
    message(FATAL_ERROR "IBEX_BUILD_DIR is required")
endif()

# Apache Arrow + Parquet (standalone)
FetchContent_Declare(
    arrow
    GIT_REPOSITORY https://github.com/apache/arrow.git
    GIT_TAG        apache-arrow-15.0.2
    GIT_SHALLOW    TRUE
    SOURCE_SUBDIR  cpp
)
set(ARROW_BUILD_SHARED ON CACHE BOOL "" FORCE)
set(ARROW_BUILD_STATIC OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_UTILITIES OFF CACHE BOOL "" FORCE)
set(ARROW_PARQUET ON CACHE BOOL "" FORCE)
set(ARROW_IPC ON CACHE BOOL "" FORCE)
set(ARROW_SIMD_LEVEL "NONE" CACHE STRING "" FORCE)
set(ARROW_RUNTIME_SIMD_LEVEL "NONE" CACHE STRING "" FORCE)
set(ARROW_CXXFLAGS "-Wno-deprecated-literal-operator -Wno-error=deprecated-literal-operator -Wno-documentation -Wno-error=documentation -Wno-deprecated-declarations -Wno-error=deprecated-declarations" CACHE STRING "" FORCE)
set(ARROW_WITH_THRIFT ON CACHE BOOL "" FORCE)
set(Thrift_SOURCE "BUNDLED" CACHE STRING "" FORCE)
set(ARROW_WITH_ZLIB OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_ZSTD OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_BROTLI OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_LZ4 OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_SNAPPY ON CACHE BOOL "" FORCE)
set(Snappy_SOURCE "BUNDLED" CACHE STRING "" FORCE)
set(Boost_SOURCE "BUNDLED" CACHE STRING "" FORCE)
set(BOOST_SOURCE "BUNDLED" CACHE STRING "" FORCE)
FetchContent_MakeAvailable(arrow)

add_library(ibex_parquet_plugin SHARED
    ${IBEX_ROOT}/libs/parquet/parquet.cpp
)

set_target_properties(ibex_parquet_plugin PROPERTIES
    OUTPUT_NAME parquet
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_ROOT}/libraries"
)

target_include_directories(ibex_parquet_plugin
    PRIVATE
        "${IBEX_ROOT}/libs/parquet"
        "${IBEX_ROOT}/include"
        "${IBEX_BUILD_DIR}/_deps/fmt-src/include"
        "${IBEX_BUILD_DIR}/_deps/spdlog-src/include"
        "${IBEX_BUILD_DIR}/_deps/robin_hood-src/src/include"
        "${arrow_SOURCE_DIR}/cpp/src"
        "${arrow_SOURCE_DIR}/cpp/src/generated"
        "${arrow_BINARY_DIR}/src"
)

set(_ibex_fmt_lib "${IBEX_BUILD_DIR}/_deps/fmt-build/libfmt.a")
if(NOT EXISTS "${_ibex_fmt_lib}")
    set(_ibex_fmt_lib "${IBEX_BUILD_DIR}/_deps/fmt-build/libfmtd.a")
endif()
set(_ibex_spdlog_lib "${IBEX_BUILD_DIR}/_deps/spdlog-build/libspdlog.a")
if(NOT EXISTS "${_ibex_spdlog_lib}")
    set(_ibex_spdlog_lib "${IBEX_BUILD_DIR}/_deps/spdlog-build/libspdlogd.a")
endif()

target_link_libraries(ibex_parquet_plugin
    PRIVATE
        ${IBEX_BUILD_DIR}/src/runtime/libibex_runtime.a
        ${IBEX_BUILD_DIR}/src/ir/libibex_ir.a
        ${IBEX_BUILD_DIR}/src/core/libibex_core.a
        ${_ibex_fmt_lib}
        ${_ibex_spdlog_lib}
)

if(TARGET arrow_shared AND TARGET parquet_shared)
    target_link_libraries(ibex_parquet_plugin PRIVATE arrow_shared parquet_shared)
    set(_ibex_arrow_rpath "$<TARGET_FILE_DIR:arrow_shared>;$<TARGET_FILE_DIR:parquet_shared>")
    if(TARGET arrow_ipc_shared)
        target_link_libraries(ibex_parquet_plugin PRIVATE arrow_ipc_shared)
        string(APPEND _ibex_arrow_rpath ";$<TARGET_FILE_DIR:arrow_ipc_shared>")
    endif()
    set_target_properties(ibex_parquet_plugin PROPERTIES
        BUILD_RPATH "${_ibex_arrow_rpath}"
    )
else()
    message(FATAL_ERROR "Arrow shared targets not found (arrow_shared/parquet_shared).")
endif()
