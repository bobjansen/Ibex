find_package(SQLite3 REQUIRED)

add_library(ibex_sqlite_plugin SHARED sqlite.cpp)

set_target_properties(ibex_sqlite_plugin PROPERTIES
    OUTPUT_NAME sqlite
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_sqlite_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
)

target_link_libraries(ibex_sqlite_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
        SQLite::SQLite3
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_sqlite_plugin PRIVATE Ibex::Sanitizers)
endif()

# Copy the library stub to the plugin output directory so that
# `import "sqlite";` works when IBEX_LIBRARY_PATH points to that directory.
add_custom_command(TARGET ibex_sqlite_plugin POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/sqlite.ibex"
        "${IBEX_PLUGIN_OUTPUT_DIR}/sqlite.ibex"
    COMMENT "Installing sqlite.ibex stub"
)
