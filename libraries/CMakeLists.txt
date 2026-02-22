add_library(ibex_csv_plugin SHARED csv.cpp)

set_target_properties(ibex_csv_plugin PROPERTIES
    OUTPUT_NAME csv
    PREFIX ""
)

target_include_directories(ibex_csv_plugin
    PRIVATE
        "${PROJECT_SOURCE_DIR}/libraries"
        "${PROJECT_SOURCE_DIR}/include"
)

target_link_libraries(ibex_csv_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_csv_plugin PRIVATE Ibex::Sanitizers)
endif()
