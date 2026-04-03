cmake_minimum_required(VERSION 3.26)

if(POLICY CMP0177)
    cmake_policy(SET CMP0177 NEW)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(IBEX_PARQUET_STANDALONE OFF)
if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(IBEX_PARQUET_STANDALONE ON)
    project(ibex_parquet_plugin LANGUAGES CXX)
    set(CMAKE_CXX_STANDARD 23)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)

    if(NOT CMAKE_BUILD_TYPE)
        set(CMAKE_BUILD_TYPE Debug CACHE STRING "" FORCE)
    endif()

    if(NOT CMAKE_INSTALL_PREFIX)
        set(CMAKE_INSTALL_PREFIX "${CMAKE_BINARY_DIR}/_install" CACHE PATH "" FORCE)
    endif()
endif()

include(FetchContent)

if(IBEX_PARQUET_STANDALONE)
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

    set(_ibex_parquet_output_dir "${IBEX_ROOT}/libraries")
else()
    set(IBEX_ROOT "${PROJECT_SOURCE_DIR}")
    set(IBEX_BUILD_DIR "${PROJECT_BINARY_DIR}")
    set(_ibex_parquet_output_dir "${IBEX_PLUGIN_OUTPUT_DIR}")
endif()

set(_ibex_need_matching_clang_c OFF)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|AppleClang")
    if(NOT DEFINED CMAKE_C_COMPILER OR CMAKE_C_COMPILER STREQUAL "")
        set(_ibex_need_matching_clang_c ON)
    elseif(NOT CMAKE_C_COMPILER_ID MATCHES "Clang|AppleClang")
        set(_ibex_need_matching_clang_c ON)
    endif()
endif()

if(_ibex_need_matching_clang_c)
    get_filename_component(_ibex_cxx_compiler_dir "${CMAKE_CXX_COMPILER}" DIRECTORY)
    find_program(_ibex_matching_clang_c
        NAMES clang
        PATHS "${_ibex_cxx_compiler_dir}"
    )
    if(_ibex_matching_clang_c)
        set(CMAKE_C_COMPILER "${_ibex_matching_clang_c}" CACHE FILEPATH
            "C compiler used for parquet dependency builds" FORCE)
    endif()
endif()

FetchContent_Declare(
    arrow
    GIT_REPOSITORY https://github.com/apache/arrow.git
    GIT_TAG        apache-arrow-22.0.0
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
set(BUILD_WARNING_LEVEL "PRODUCTION" CACHE STRING "" FORCE)
set(BUILD_WARNING_FLAGS "-w" CACHE STRING "" FORCE)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|AppleClang")
    set(ARROW_CXXFLAGS "-Wno-deprecated-literal-operator -Wno-error=deprecated-literal-operator -Wno-documentation -Wno-error=documentation -Wno-deprecated-declarations -Wno-error=deprecated-declarations" CACHE STRING "" FORCE)
else()
    set(ARROW_CXXFLAGS "" CACHE STRING "" FORCE)
endif()
set(_ibex_saved_suppress_dev_warnings "${CMAKE_SUPPRESS_DEVELOPER_WARNINGS}")
set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS 1)
set(ARROW_WITH_THRIFT ON CACHE BOOL "" FORCE)
set(Thrift_SOURCE "BUNDLED" CACHE STRING "" FORCE)
set(ARROW_WITH_ZLIB ON CACHE BOOL "" FORCE)
set(ZLIB_SOURCE "BUNDLED" CACHE STRING "" FORCE)
set(ARROW_WITH_ZSTD OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_BROTLI OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_LZ4 OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_SNAPPY ON CACHE BOOL "" FORCE)
set(Snappy_SOURCE "BUNDLED" CACHE STRING "" FORCE)
set(ARROW_WITH_RAPIDJSON ON CACHE BOOL "" FORCE)
set(RapidJSON_SOURCE "BUNDLED" CACHE STRING "" FORCE)
set(Boost_SOURCE "BUNDLED" CACHE STRING "" FORCE)
set(BOOST_SOURCE "BUNDLED" CACHE STRING "" FORCE)
FetchContent_MakeAvailable(arrow)
set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS "${_ibex_saved_suppress_dev_warnings}")

# Bundled thrift uses deprecated std::iterator; suppress the warning on its target.
if(TARGET thrift)
    target_compile_options(thrift PRIVATE -Wno-deprecated-declarations)
endif()

add_library(ibex_parquet_plugin SHARED parquet.cpp)

target_compile_options(ibex_parquet_plugin
    PRIVATE
        $<$<CXX_COMPILER_ID:Clang>:-Wno-missing-designated-field-initializers>
        $<$<CXX_COMPILER_ID:AppleClang>:-Wno-missing-field-initializers>
)

set_target_properties(ibex_parquet_plugin PROPERTIES
    OUTPUT_NAME parquet
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${_ibex_parquet_output_dir}"
    RUNTIME_OUTPUT_DIRECTORY "${_ibex_parquet_output_dir}"
)

target_include_directories(ibex_parquet_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${IBEX_ROOT}/include"
)

target_include_directories(ibex_parquet_plugin
    SYSTEM PRIVATE
        "${arrow_SOURCE_DIR}/cpp/src"
        "${arrow_SOURCE_DIR}/cpp/src/generated"
        "${arrow_BINARY_DIR}/src"
)

if(IBEX_PARQUET_STANDALONE)
    target_include_directories(ibex_parquet_plugin
        PRIVATE
            "${IBEX_BUILD_DIR}/_deps/fmt-src/include"
            "${IBEX_BUILD_DIR}/_deps/spdlog-src/include"
            "${IBEX_BUILD_DIR}/_deps/robin_hood-src/src/include"
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
else()
    target_link_libraries(ibex_parquet_plugin
        PRIVATE
            Ibex::runtime
            Ibex::CompilerOptions
    )

    if(TARGET Ibex::Sanitizers)
        target_link_libraries(ibex_parquet_plugin PRIVATE Ibex::Sanitizers)
    endif()
endif()

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

add_custom_command(TARGET ibex_parquet_plugin POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/parquet.ibex"
        "${_ibex_parquet_output_dir}/parquet.ibex"
    COMMENT "Installing parquet.ibex stub"
)
