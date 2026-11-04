# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# QuietThirdParty.cmake — silence configure-time warnings from vendored
# FetchContent dependencies without hiding Ibex's own.
#
#   ibex_begin_third_party()
#   FetchContent_MakeAvailable(foo)
#   ibex_end_third_party()
#
# Between the two calls:
#   * deprecation warnings are off — e.g. "Compatibility with CMake < 3.10"
#     from an old cmake_minimum_required(). CMake only honours
#     CMAKE_WARN_DEPRECATED as a *cache* entry, so the user's value is saved
#     and restored around the block;
#   * author warnings from message(AUTHOR_WARNING) are off;
#   * policies that dependencies leave unset and then trip a "Policy CMPxxxx
#     is not set" dev warning on default to OLD. An unset policy already gets
#     OLD behaviour, so this only drops the warning, never changes the build.
#
# Plain message(WARNING) calls in a dependency cannot be silenced this way.
#
# Macros, not functions: FetchContent_MakeAvailable must run in the caller's
# scope, so the settings have to be made (and undone) there too.

include_guard(GLOBAL)

set(_IBEX_THIRD_PARTY_OLD_POLICIES
    CMP0148  # FindPythonInterp / FindPythonLibs removed (bundled Thrift)
    CMP0177  # install() DESTINATION normalization (Arrow)
)

macro(ibex_begin_third_party)
    if(DEFINED CACHE{CMAKE_WARN_DEPRECATED})
        set(_ibex_tp_saved_warn_deprecated "$CACHE{CMAKE_WARN_DEPRECATED}")
    else()
        set(_ibex_tp_saved_warn_deprecated "__unset__")
    endif()
    set(CMAKE_WARN_DEPRECATED OFF CACHE BOOL "" FORCE)

    set(_ibex_tp_saved_suppress_dev "${CMAKE_SUPPRESS_DEVELOPER_WARNINGS}")
    set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS 1)

    foreach(_ibex_tp_policy IN LISTS _IBEX_THIRD_PARTY_OLD_POLICIES)
        set(CMAKE_POLICY_DEFAULT_${_ibex_tp_policy} OLD)
    endforeach()
endmacro()

macro(ibex_end_third_party)
    foreach(_ibex_tp_policy IN LISTS _IBEX_THIRD_PARTY_OLD_POLICIES)
        unset(CMAKE_POLICY_DEFAULT_${_ibex_tp_policy})
    endforeach()

    set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS "${_ibex_tp_saved_suppress_dev}")

    if(_ibex_tp_saved_warn_deprecated STREQUAL "__unset__")
        unset(CMAKE_WARN_DEPRECATED CACHE)
    else()
        set(CMAKE_WARN_DEPRECATED "${_ibex_tp_saved_warn_deprecated}"
            CACHE BOOL "" FORCE)
    endif()
endmacro()
