add_library(ibex_adbc_plugin SHARED adbc.cpp)

find_path(ADBC_INCLUDE_DIR
    NAMES adbc.h
)

find_library(ADBC_DRIVER_MANAGER_LIBRARY
    NAMES adbc_driver_manager libadbc_driver_manager
)

if(NOT ADBC_INCLUDE_DIR OR NOT ADBC_DRIVER_MANAGER_LIBRARY)
    message(FATAL_ERROR
        "IBEX_BUILD_ADBC=ON requires an installed ADBC driver manager "
        "(missing adbc.h or libadbc_driver_manager)."
    )
endif()

set_target_properties(ibex_adbc_plugin PROPERTIES
    OUTPUT_NAME adbc
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_adbc_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
        "${ADBC_INCLUDE_DIR}"
)

target_link_libraries(ibex_adbc_plugin
    PRIVATE
        Ibex::interop
        Ibex::CompilerOptions
        "${ADBC_DRIVER_MANAGER_LIBRARY}"
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_adbc_plugin PRIVATE Ibex::Sanitizers)
endif()

add_custom_command(TARGET ibex_adbc_plugin POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/adbc.ibex"
        "${IBEX_PLUGIN_OUTPUT_DIR}/adbc.ibex"
    COMMENT "Installing adbc.ibex stub"
)
