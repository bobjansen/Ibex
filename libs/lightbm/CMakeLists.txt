add_library(ibex_lightbm_plugin SHARED lightbm.cpp)

set_target_properties(ibex_lightbm_plugin PROPERTIES
    OUTPUT_NAME lightbm
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_lightbm_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
)

target_link_libraries(ibex_lightbm_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_lightbm_plugin PRIVATE Ibex::Sanitizers)
endif()

add_custom_command(TARGET ibex_lightbm_plugin POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/lightbm.ibex"
        "${IBEX_PLUGIN_OUTPUT_DIR}/lightbm.ibex"
    COMMENT "Installing lightbm.ibex stub"
)
