add_library(ibex_kmeans_plugin SHARED kmeans.cpp)

set_target_properties(ibex_kmeans_plugin PROPERTIES
    OUTPUT_NAME kmeans
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_kmeans_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
)

target_link_libraries(ibex_kmeans_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_kmeans_plugin PRIVATE Ibex::Sanitizers)
endif()

add_custom_command(TARGET ibex_kmeans_plugin POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/kmeans.ibex"
        "${IBEX_PLUGIN_OUTPUT_DIR}/kmeans.ibex"
    COMMENT "Installing kmeans.ibex stub"
)
