# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

add_library(ibex_kmeans_plugin SHARED kmeans.cpp)

set_target_properties(ibex_kmeans_plugin PROPERTIES
    OUTPUT_NAME kmeans
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_kmeans_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
)

target_link_libraries(ibex_kmeans_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_kmeans_plugin PRIVATE Ibex::Sanitizers)
endif()

set(_ibex_kmeans_stub "${IBEX_PLUGIN_OUTPUT_DIR}/kmeans.ibex")

add_custom_command(
    OUTPUT "${_ibex_kmeans_stub}"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/kmeans.ibex"
        "${_ibex_kmeans_stub}"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/kmeans.ibex"
    COMMENT "Installing kmeans.ibex stub"
)
add_custom_target(ibex_kmeans_stub DEPENDS "${_ibex_kmeans_stub}")
add_dependencies(ibex_kmeans_plugin ibex_kmeans_stub)
