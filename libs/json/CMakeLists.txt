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

# Copy the library stub to the plugin output directory so that
# `import "json";` works when IBEX_LIBRARY_PATH points to that directory.
add_custom_command(TARGET ibex_json_plugin POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/json.ibex"
        "${IBEX_PLUGIN_OUTPUT_DIR}/json.ibex"
    COMMENT "Installing json.ibex stub"
)
