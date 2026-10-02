# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# Sanitizers.cmake — ASan + UBSan for Debug builds
#
# UBSan aborts on the first report instead of printing and carrying on, so
# undefined behaviour fails the test that hit it. Without this, UB in a passing
# test went unnoticed in CI.

add_library(ibex_sanitizer_options INTERFACE)
add_library(Ibex::Sanitizers ALIAS ibex_sanitizer_options)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_options(ibex_sanitizer_options
        INTERFACE
            -fsanitize=address,undefined
            -fno-sanitize-recover=undefined
            -fno-omit-frame-pointer
    )
    target_link_options(ibex_sanitizer_options
        INTERFACE
            -fsanitize=address,undefined
    )
    message(STATUS "Ibex: AddressSanitizer + UBSan enabled (Debug)")
endif()
