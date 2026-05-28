add_library(ibex_json_plugin SHARED json.cpp)

set_target_properties(ibex_json_plugin PROPERTIES
    OUTPUT_NAME json
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_json_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
)

target_link_libraries(ibex_json_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
        nlohmann_json::nlohmann_json
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_json_plugin PRIVATE Ibex::Sanitizers)
endif()

set(_ibex_json_stub "${IBEX_PLUGIN_OUTPUT_DIR}/json.ibex")

# Copy the library stub to the plugin output directory so that
# `import "json";` works when IBEX_LIBRARY_PATH points to that directory.
add_custom_command(
    OUTPUT "${_ibex_json_stub}"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/json.ibex"
        "${_ibex_json_stub}"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/json.ibex"
    COMMENT "Installing json.ibex stub"
)
add_custom_target(ibex_json_stub DEPENDS "${_ibex_json_stub}")
add_dependencies(ibex_json_plugin ibex_json_stub)
