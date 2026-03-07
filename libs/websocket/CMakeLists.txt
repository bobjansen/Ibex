add_library(ibex_websocket_plugin SHARED websocket.cpp)

set_target_properties(ibex_websocket_plugin PROPERTIES
    OUTPUT_NAME websocket
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_websocket_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
)

target_link_libraries(ibex_websocket_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_websocket_plugin PRIVATE Ibex::Sanitizers)
endif()

# Copy the library stub to the plugin output directory so that
# `import "websocket";` works when IBEX_LIBRARY_PATH points to that directory.
add_custom_command(TARGET ibex_websocket_plugin POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/websocket.ibex"
        "${IBEX_PLUGIN_OUTPUT_DIR}/websocket.ibex"
    COMMENT "Installing websocket.ibex stub"
)
