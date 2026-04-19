add_library(ibex_kafka_plugin SHARED kafka.cpp)

find_path(KAFKA_INCLUDE_DIR
    NAMES librdkafka/rdkafka.h
)

find_library(KAFKA_LIBRARY
    NAMES rdkafka librdkafka
)

if(NOT KAFKA_INCLUDE_DIR OR NOT KAFKA_LIBRARY)
    message(FATAL_ERROR
        "IBEX_BUILD_KAFKA=ON requires an installed librdkafka development package "
        "(missing librdkafka/rdkafka.h or librdkafka)."
    )
endif()

set_target_properties(ibex_kafka_plugin PROPERTIES
    OUTPUT_NAME kafka
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_kafka_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
        "${KAFKA_INCLUDE_DIR}"
)

target_link_libraries(ibex_kafka_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
        nlohmann_json::nlohmann_json
        "${KAFKA_LIBRARY}"
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_kafka_plugin PRIVATE Ibex::Sanitizers)
endif()

add_custom_command(TARGET ibex_kafka_plugin POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/kafka.ibex"
        "${IBEX_PLUGIN_OUTPUT_DIR}/kafka.ibex"
    COMMENT "Installing kafka.ibex stub"
)
