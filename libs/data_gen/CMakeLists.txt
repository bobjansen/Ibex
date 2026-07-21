add_library(ibex_data_gen_plugin SHARED data_gen.cpp)

set_target_properties(ibex_data_gen_plugin PROPERTIES
    OUTPUT_NAME data_gen
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_data_gen_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
)

target_link_libraries(ibex_data_gen_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_data_gen_plugin PRIVATE Ibex::Sanitizers)
endif()

set(_ibex_data_gen_stub "${IBEX_PLUGIN_OUTPUT_DIR}/data_gen.ibex")

# Copy the library stub to the plugin output directory so that
# `import data_gen;` works when IBEX_LIBRARY_PATH points to that directory.
add_custom_command(
    OUTPUT "${_ibex_data_gen_stub}"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/data_gen.ibex"
        "${_ibex_data_gen_stub}"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/data_gen.ibex"
    COMMENT "Installing data_gen.ibex stub"
)
add_custom_target(ibex_data_gen_stub DEPENDS "${_ibex_data_gen_stub}")
add_dependencies(ibex_data_gen_plugin ibex_data_gen_stub)
