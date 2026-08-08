# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# Sanitizers.cmake — ASan + UBSan for Debug builds

add_library(ibex_sanitizer_options INTERFACE)
add_library(Ibex::Sanitizers ALIAS ibex_sanitizer_options)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_options(ibex_sanitizer_options
        INTERFACE
            -fsanitize=address,undefined
            -fno-omit-frame-pointer
    )
    target_link_options(ibex_sanitizer_options
        INTERFACE
            -fsanitize=address,undefined
    )
    message(STATUS "Ibex: AddressSanitizer + UBSan enabled (Debug)")
endif()
