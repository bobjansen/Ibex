# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

add_library(ibex_parser STATIC
    effects.cpp
    lexer.cpp
    lower.cpp
    parser.cpp
)
add_library(Ibex::parser ALIAS ibex_parser)

target_include_directories(ibex_parser
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(ibex_parser
    PUBLIC
        Ibex::ir
    PRIVATE
        Ibex::CompilerOptions
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_parser PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_parser
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
