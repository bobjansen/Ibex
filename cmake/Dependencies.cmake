# Dependencies.cmake — External dependencies via FetchContent

include(FetchContent)

# fmt — modern formatting library
FetchContent_Declare(
    fmt
    GIT_REPOSITORY https://github.com/fmtlib/fmt.git
    GIT_TAG        11.1.4
    GIT_SHALLOW    TRUE
)

# spdlog — structured logging (uses fmt)
FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.15.1
    GIT_SHALLOW    TRUE
)

# CLI11 — command-line parsing
FetchContent_Declare(
    CLI11
    GIT_REPOSITORY https://github.com/CLIUtils/CLI11.git
    GIT_TAG        v2.4.2
    GIT_SHALLOW    TRUE
)

# Use external fmt for spdlog
set(SPDLOG_FMT_EXTERNAL ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(fmt spdlog CLI11)

# robin-hood-hashing — fast open-addressing hash map (header-only)
FetchContent_Declare(
    robin_hood
    GIT_REPOSITORY https://github.com/martinus/robin-hood-hashing.git
    GIT_TAG        3.11.5
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(robin_hood)

# Apache Arrow + Parquet (for parquet plugin)
FetchContent_Declare(
    arrow
    GIT_REPOSITORY https://github.com/apache/arrow.git
    GIT_TAG        apache-arrow-15.0.2
    GIT_SHALLOW    TRUE
    SOURCE_SUBDIR  cpp
)
set(ARROW_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(ARROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_UTILITIES OFF CACHE BOOL "" FORCE)
set(ARROW_PARQUET ON CACHE BOOL "" FORCE)
set(ARROW_SIMD_LEVEL "NONE" CACHE STRING "" FORCE)
set(ARROW_RUNTIME_SIMD_LEVEL "NONE" CACHE STRING "" FORCE)
set(ARROW_CXXFLAGS "-Wno-deprecated-literal-operator -Wno-error=deprecated-literal-operator -Wno-documentation -Wno-error=documentation" CACHE STRING "" FORCE)
set(ARROW_BUILD_SHARED ON CACHE BOOL "" FORCE)
set(ARROW_BUILD_STATIC OFF CACHE BOOL "" FORCE)
set(ARROW_IPC ON CACHE BOOL "" FORCE)
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

# Catch2 — testing framework (only when tests enabled)
if(IBEX_BUILD_TESTS)
    FetchContent_Declare(
        Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG        v3.7.1
        GIT_SHALLOW    TRUE
    )
    FetchContent_MakeAvailable(Catch2)
    list(APPEND CMAKE_MODULE_PATH "${Catch2_SOURCE_DIR}/extras")
endif()
