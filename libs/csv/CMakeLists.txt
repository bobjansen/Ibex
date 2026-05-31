# CURL powers optional HTTPS support in read_csv. It's not available on every
# CI runner (Windows, the minimal clang-werror box), so treat it as optional:
# build the plugin without HTTPS when it's missing rather than failing configure.
find_package(CURL)

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
        fast_float
)

if(CURL_FOUND)
    target_link_libraries(ibex_csv_plugin PRIVATE CURL::libcurl)
    target_compile_definitions(ibex_csv_plugin PRIVATE IBEX_CSV_HAVE_CURL)
else()
    message(STATUS "CURL not found: building csv plugin without HTTPS read_csv support")
endif()

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_csv_plugin PRIVATE Ibex::Sanitizers)
endif()

set(_ibex_csv_stub "${IBEX_PLUGIN_OUTPUT_DIR}/csv.ibex")

# Copy the library stub to the plugin output directory so that
# `import "csv";` works when IBEX_LIBRARY_PATH points to that directory.
add_custom_command(
    OUTPUT "${_ibex_csv_stub}"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/csv.ibex"
        "${_ibex_csv_stub}"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/csv.ibex"
    COMMENT "Installing csv.ibex stub"
)
add_custom_target(ibex_csv_stub DEPENDS "${_ibex_csv_stub}")
add_dependencies(ibex_csv_plugin ibex_csv_stub)
