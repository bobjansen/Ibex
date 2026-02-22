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
