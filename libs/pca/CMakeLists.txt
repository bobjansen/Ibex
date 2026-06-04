add_library(ibex_pca_plugin SHARED pca.cpp)

set_target_properties(ibex_pca_plugin PROPERTIES
    OUTPUT_NAME pca
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_pca_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
)

target_link_libraries(ibex_pca_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_pca_plugin PRIVATE Ibex::Sanitizers)
endif()

add_custom_command(TARGET ibex_pca_plugin POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/pca.ibex"
        "${IBEX_PLUGIN_OUTPUT_DIR}/pca.ibex"
    COMMENT "Installing pca.ibex stub"
)
