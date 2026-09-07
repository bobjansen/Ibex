# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

add_library(ibex_fs_plugin SHARED fs.cpp)

set_target_properties(ibex_fs_plugin PROPERTIES
    OUTPUT_NAME fs
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_fs_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
)

target_link_libraries(ibex_fs_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_fs_plugin PRIVATE Ibex::Sanitizers)
endif()

set(_ibex_fs_stub "${IBEX_PLUGIN_OUTPUT_DIR}/fs.ibex")

# Copy the library stub to the plugin output directory so that
# `import "fs";` works when IBEX_LIBRARY_PATH points to that directory.
add_custom_command(
    OUTPUT "${_ibex_fs_stub}"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/fs.ibex"
        "${_ibex_fs_stub}"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/fs.ibex"
    COMMENT "Installing fs.ibex stub"
)
add_custom_target(ibex_fs_stub DEPENDS "${_ibex_fs_stub}")
add_dependencies(ibex_fs_plugin ibex_fs_stub)
