# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

add_executable(ibex_example_basic basic.cpp)

target_link_libraries(ibex_example_basic
    PRIVATE
        Ibex::core
        Ibex::ir
        Ibex::runtime
        Ibex::CompilerOptions
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_example_basic PRIVATE Ibex::Sanitizers)
endif()
