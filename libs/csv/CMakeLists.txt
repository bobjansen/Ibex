add_library(ibex_csv_plugin SHARED csv.cpp)

set_target_properties(ibex_csv_plugin PROPERTIES
    OUTPUT_NAME csv
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_csv_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
)

target_link_libraries(ibex_csv_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
        rapidcsv
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_csv_plugin PRIVATE Ibex::Sanitizers)
endif()

# Copy the library stub to the plugin output directory so that
# `import "csv";` works when IBEX_LIBRARY_PATH points to that directory.
add_custom_command(TARGET ibex_csv_plugin POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/csv.ibex"
        "${IBEX_PLUGIN_OUTPUT_DIR}/csv.ibex"
    COMMENT "Installing csv.ibex stub"
)
