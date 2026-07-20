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
find_package(CURL REQUIRED)

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
set(ARROW_FILESYSTEM ON CACHE BOOL "" FORCE)
set(ARROW_PARQUET ON CACHE BOOL "" FORCE)
set(ARROW_IPC ON CACHE BOOL "" FORCE)
# S3 filesystem support drags in the entire bundled AWS SDK (aws-lc, aws-c-*),
# a large from-source compile that needs libssl-dev. Real ibex builds keep it
# (default ON); the benchmark harness has no use for S3 reads and disables it
# via -DIBEX_PARQUET_S3=OFF to cut build time.
option(IBEX_PARQUET_S3 "Build the Parquet/Arrow stack with S3 filesystem support" ON)
set(ARROW_S3 ${IBEX_PARQUET_S3} CACHE BOOL "" FORCE)
set(ARROW_S3_MODULE OFF CACHE BOOL "" FORCE)
# Arrow's SIMD kernels, selected by runtime CPU detection.
#
# ARROW_SIMD_LEVEL is the *compile-time baseline*, and DEFAULT resolves per
# architecture (SSE4.2 on x86-64, NEON on ARM) — so it stays portable, unlike
# pinning it to AVX2, which would compile those objects -march=haswell and give
# the binary a hardware floor. ARROW_RUNTIME_SIMD_LEVEL=MAX then compiles the
# AVX2/AVX-512 kernels as runtime-dispatched alternatives, picked only on CPUs
# that actually have them.
#
# These were "NONE" from the plugin's first commit, with no recorded reason —
# almost certainly just to sidestep the xsimd dependency below, which Arrow
# needs as soon as SIMD is on and which nothing else fetches. Turning them back
# on is worth ~5-11% end-to-end on PDS-H, since Parquet decode is what dominates
# the scan-bound queries.
set(xsimd_SOURCE "BUNDLED" CACHE STRING "" FORCE)
set(ARROW_SIMD_LEVEL "DEFAULT" CACHE STRING "" FORCE)
set(ARROW_RUNTIME_SIMD_LEVEL "MAX" CACHE STRING "" FORCE)
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
set(ZLIB_SOURCE "SYSTEM" CACHE STRING "" FORCE)
# See cmake/ThriftZlibFix.cmake: on Windows, under the vcpkg toolchain,
# ZLIB_LIBRARY ends up cached in multi-config list form ("optimized;<path>;
# debug;<path>") by the time bundled Thrift's DefineOptions.cmake reads it,
# which crashes its `file(TO_CMAKE_PATH ...)` call. The hook normalizes that
# value to a single path right as Thrift's own project() call runs, so it's
# immune to exactly when/what populated the cache.
if(WIN32)
    set(CMAKE_PROJECT_thrift_INCLUDE_BEFORE "${IBEX_ROOT}/cmake/ThriftZlibFix.cmake")
endif()
set(ARROW_WITH_ZSTD ON CACHE BOOL "" FORCE)
set(zstd_SOURCE "BUNDLED" CACHE STRING "" FORCE)
set(ZSTD_SOURCE "BUNDLED" CACHE STRING "" FORCE)
set(ARROW_WITH_BROTLI OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_LZ4 OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_SNAPPY ON CACHE BOOL "" FORCE)
set(Snappy_SOURCE "BUNDLED" CACHE STRING "" FORCE)
set(ARROW_WITH_RAPIDJSON ON CACHE BOOL "" FORCE)
set(RapidJSON_SOURCE "BUNDLED" CACHE STRING "" FORCE)
set(Boost_SOURCE "BUNDLED" CACHE STRING "" FORCE)
set(BOOST_SOURCE "BUNDLED" CACHE STRING "" FORCE)
set(AWSSDK_SOURCE "BUNDLED" CACHE STRING "" FORCE)
set(CMAKE_PROJECT_aws-cpp-sdk-all_INCLUDE_BEFORE
    "${IBEX_ROOT}/cmake/AwsSdkCppModulePath.cmake")
set(UNSAFE_TREAT_WARNINGS_AS_ERRORS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(arrow)
set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS "${_ibex_saved_suppress_dev_warnings}")

# Bundled thrift uses deprecated std::iterator; suppress the warning on its target.
if(TARGET thrift)
    target_compile_options(thrift PRIVATE -Wno-deprecated-declarations)
endif()

add_library(ibex_parquet_plugin SHARED parquet.cpp)

# -Wno-missing-designated-field-initializers only exists from Clang 19
# onwards (the release workflow's Linux leg still pins clang-18), so fall
# back to the older, broader -Wno-missing-field-initializers there instead of
# erroring on an unknown flag under -Werror.
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 19)
    target_compile_options(ibex_parquet_plugin PRIVATE -Wno-missing-designated-field-initializers)
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang|AppleClang")
    target_compile_options(ibex_parquet_plugin PRIVATE -Wno-missing-field-initializers)
endif()

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
    target_link_libraries(ibex_parquet_plugin PRIVATE arrow_shared parquet_shared CURL::libcurl)
    set(_ibex_arrow_shared_targets arrow_shared parquet_shared)
    if(TARGET arrow_ipc_shared)
        # On some configs (observed on Windows) arrow_ipc_shared exists as a
        # target but isn't actually a shared/module library (e.g. IPC gets
        # folded into arrow_shared directly) -- TARGET_FILE-family generator
        # expressions hard-error on anything else, so only treat it as a
        # runtime dependency to link/copy when it really is one.
        get_target_property(_ibex_arrow_ipc_type arrow_ipc_shared TYPE)
        if(_ibex_arrow_ipc_type MATCHES "^(SHARED|MODULE)_LIBRARY$")
            target_link_libraries(ibex_parquet_plugin PRIVATE arrow_ipc_shared)
            list(APPEND _ibex_arrow_shared_targets arrow_ipc_shared)
        endif()
        unset(_ibex_arrow_ipc_type)
    endif()

    # Ship these plugins next to their arrow/parquet .so/.dylib/.dll: a release
    # bundle is copied wholesale onto another machine (or even just moved out
    # of the build tree), so an absolute BUILD_RPATH pointing at
    # _deps/arrow-build on the CI runner would dangle. $ORIGIN/@loader_path
    # instead resolves relative to wherever ibex_parquet_plugin itself ends up
    # — which works as long as its arrow/parquet deps are copied alongside it
    # (see the POST_BUILD copy below). Windows has no RPATH equivalent, but
    # doesn't need one: the default DLL search order already includes the
    # directory of the DLL being loaded, so copying arrow.dll/parquet.dll next
    # to the plugin is sufficient there.
    if(APPLE)
        set_target_properties(ibex_parquet_plugin PROPERTIES
            BUILD_RPATH "@loader_path"
        )
    elseif(NOT WIN32)
        set_target_properties(ibex_parquet_plugin PROPERTIES
            BUILD_RPATH "$ORIGIN"
        )
    endif()

    foreach(_ibex_arrow_dep IN LISTS _ibex_arrow_shared_targets)
        if(WIN32)
            # TARGET_SONAME_FILE is rejected outright ("not allowed for DLL
            # target platforms") on Windows -- there's no SONAME concept, so
            # copy the .dll itself via TARGET_FILE instead.
            set(_ibex_arrow_dep_file "$<TARGET_FILE:${_ibex_arrow_dep}>")
        else()
            # Copy the SONAME-named file (e.g. libarrow.so.2200), not
            # TARGET_FILE's fully-versioned real file (libarrow.so.2200.0.0):
            # the dynamic loader resolves ibex_parquet_plugin's DT_NEEDED
            # entries by SONAME, and `cmake -E copy` dereferences that symlink
            # into a real file under the SONAME name at the destination,
            # which is exactly the name the loader looks for.
            set(_ibex_arrow_dep_file "$<TARGET_SONAME_FILE:${_ibex_arrow_dep}>")
        endif()
        add_custom_command(TARGET ibex_parquet_plugin POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
                "${_ibex_arrow_dep_file}"
                "${_ibex_parquet_output_dir}"
            COMMENT "Copying ${_ibex_arrow_dep} runtime library next to ibex_parquet_plugin"
        )
    endforeach()
    unset(_ibex_arrow_dep_file)
else()
    message(FATAL_ERROR "Arrow shared targets not found (arrow_shared/parquet_shared).")
endif()

set(_ibex_parquet_stub "${_ibex_parquet_output_dir}/parquet.ibex")
add_custom_command(
    OUTPUT "${_ibex_parquet_stub}"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/parquet.ibex"
        "${_ibex_parquet_stub}"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/parquet.ibex"
    COMMENT "Installing parquet.ibex stub"
)
add_custom_target(ibex_parquet_stub DEPENDS "${_ibex_parquet_stub}")
add_dependencies(ibex_parquet_plugin ibex_parquet_stub)
