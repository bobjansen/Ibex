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
