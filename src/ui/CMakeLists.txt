# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

add_library(ibex_ui STATIC
    server.cpp
)
add_library(Ibex::ui ALIAS ibex_ui)

target_include_directories(ibex_ui
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(ibex_ui
    PUBLIC
        Ibex::repl
    PRIVATE
        Ibex::CompilerOptions
        nlohmann_json::nlohmann_json
)

if(WIN32)
    target_link_libraries(ibex_ui PRIVATE ws2_32)
endif()

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_ui PRIVATE Ibex::Sanitizers)
endif()

find_program(IBEX_NPM_EXECUTABLE NAMES npm REQUIRED)
set(IBEX_UI_SOURCE_DIR "${PROJECT_SOURCE_DIR}/ui")
set(IBEX_UI_OUTPUT_DIR "${IBEX_PLUGIN_OUTPUT_DIR}/ui")
set(IBEX_UI_INDEX "${IBEX_UI_OUTPUT_DIR}/index.html")

add_custom_command(
    OUTPUT "${IBEX_UI_INDEX}"
    COMMAND "${IBEX_NPM_EXECUTABLE}" ci --no-audit --no-fund
    COMMAND "${IBEX_NPM_EXECUTABLE}" run build
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${IBEX_UI_OUTPUT_DIR}"
    COMMAND "${CMAKE_COMMAND}" -E copy_directory "${IBEX_UI_SOURCE_DIR}/dist" "${IBEX_UI_OUTPUT_DIR}"
    WORKING_DIRECTORY "${IBEX_UI_SOURCE_DIR}"
    DEPENDS
        "${IBEX_UI_SOURCE_DIR}/package.json"
        "${IBEX_UI_SOURCE_DIR}/package-lock.json"
        "${IBEX_UI_SOURCE_DIR}/index.html"
        "${IBEX_UI_SOURCE_DIR}/src/main.tsx"
        "${IBEX_UI_SOURCE_DIR}/src/styles.css"
    COMMENT "Building bundled Ibex browser UI"
    VERBATIM
)
add_custom_target(ibex_ui_assets DEPENDS "${IBEX_UI_INDEX}")
