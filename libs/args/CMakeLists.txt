# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

add_library(ibex_args_plugin SHARED args.cpp)

set_target_properties(ibex_args_plugin PROPERTIES
    OUTPUT_NAME args
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_args_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
)

target_link_libraries(ibex_args_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_args_plugin PRIVATE Ibex::Sanitizers)
endif()

set(_ibex_args_stub "${IBEX_PLUGIN_OUTPUT_DIR}/args.ibex")

# Copy the library stub to the plugin output directory so that
# `import "args";` works when IBEX_LIBRARY_PATH points to that directory.
add_custom_command(
    OUTPUT "${_ibex_args_stub}"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/args.ibex"
        "${_ibex_args_stub}"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/args.ibex"
    COMMENT "Installing args.ibex stub"
)
add_custom_target(ibex_args_stub DEPENDS "${_ibex_args_stub}")
add_dependencies(ibex_args_plugin ibex_args_stub)
