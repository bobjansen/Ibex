# Dependencies.cmake — External dependencies via FetchContent

include(FetchContent)

function(ibex_mark_target_system_headers target_name)
    if(NOT TARGET "${target_name}")
        return()
    endif()

    get_target_property(_ibex_aliased_target "${target_name}" ALIASED_TARGET)
    if(_ibex_aliased_target AND NOT _ibex_aliased_target STREQUAL "_ibex_aliased_target-NOTFOUND")
        set(target_name "${_ibex_aliased_target}")
    endif()

    set_property(TARGET "${target_name}" PROPERTY SYSTEM ON)
endfunction()

function(ibex_silence_external_target target_name)
    if(NOT TARGET "${target_name}")
        return()
    endif()

    get_target_property(_ibex_aliased_target "${target_name}" ALIASED_TARGET)
    if(_ibex_aliased_target AND NOT _ibex_aliased_target STREQUAL "_ibex_aliased_target-NOTFOUND")
        set(target_name "${_ibex_aliased_target}")
    endif()

    ibex_mark_target_system_headers("${target_name}")

    get_target_property(_ibex_target_type "${target_name}" TYPE)
    if(_ibex_target_type AND NOT _ibex_target_type STREQUAL "INTERFACE_LIBRARY")
        target_compile_options("${target_name}" PRIVATE
            $<$<AND:$<COMPILE_LANGUAGE:C>,$<NOT:$<CXX_COMPILER_ID:MSVC>>>:-w>
            $<$<AND:$<COMPILE_LANGUAGE:CXX>,$<NOT:$<CXX_COMPILER_ID:MSVC>>>:-w>
        )
        # On MSVC, don't add /w — the SYSTEM property already suppresses
        # warnings in consuming targets' includes, and silencing the library's
        # own compilation isn't worth fighting the D9025 warning from the VS
        # generator's default WarningLevel.
    endif()
endfunction()

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
ibex_silence_external_target(fmt)
ibex_silence_external_target(spdlog)
ibex_silence_external_target(CLI11)

# robin-hood-hashing — fast open-addressing hash map (header-only)
FetchContent_Declare(
    robin_hood
    GIT_REPOSITORY https://github.com/martinus/robin-hood-hashing.git
    GIT_TAG        3.11.5
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(robin_hood)
ibex_mark_target_system_headers(robin_hood::robin_hood)

# nlohmann/json — header-only JSON library (MIT)
FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG        v3.11.3
    GIT_SHALLOW    TRUE
)
set(JSON_BuildTests OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(nlohmann_json)
ibex_mark_target_system_headers(nlohmann_json::nlohmann_json)

# Catch2 — testing framework (only when tests enabled)
if(IBEX_BUILD_TESTS)
    FetchContent_Declare(
        Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG        v3.7.1
        GIT_SHALLOW    TRUE
    )
    FetchContent_MakeAvailable(Catch2)
    ibex_silence_external_target(Catch2)
    ibex_silence_external_target(Catch2WithMain)
    list(APPEND CMAKE_MODULE_PATH "${Catch2_SOURCE_DIR}/extras")
endif()
