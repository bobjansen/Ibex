add_library(ibex_udp_plugin SHARED udp.cpp)

set_target_properties(ibex_udp_plugin PROPERTIES
    OUTPUT_NAME udp
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_udp_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
)

target_link_libraries(ibex_udp_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_udp_plugin PRIVATE Ibex::Sanitizers)
endif()

# Copy the library stub to the plugin output directory so that
# `import "udp";` works when IBEX_LIBRARY_PATH points to that directory.
add_custom_command(TARGET ibex_udp_plugin POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/udp.ibex"
        "${IBEX_PLUGIN_OUTPUT_DIR}/udp.ibex"
    COMMENT "Installing udp.ibex stub"
)
