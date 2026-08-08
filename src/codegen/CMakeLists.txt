# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

add_library(ibex_codegen STATIC
    emitter.cpp
)
add_library(Ibex::codegen ALIAS ibex_codegen)

target_include_directories(ibex_codegen
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(ibex_codegen
    PUBLIC
        Ibex::ir
    PRIVATE
        Ibex::CompilerOptions
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_codegen PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_codegen
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
