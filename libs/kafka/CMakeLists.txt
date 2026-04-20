add_library(ibex_kafka_plugin SHARED kafka.cpp)

include(FetchContent)

find_path(KAFKA_INCLUDE_DIR
    NAMES librdkafka/rdkafka.h
)

find_library(KAFKA_LIBRARY
    NAMES rdkafka librdkafka
)

find_package(CURL REQUIRED)

set(IBEX_KAFKA_AVRO_TARGET "")
if(IBEX_KAFKA_VENDOR_AVROCPP)
    if(DEFINED fmt_BINARY_DIR AND EXISTS "${fmt_BINARY_DIR}/fmt-config.cmake")
        set(fmt_DIR "${fmt_BINARY_DIR}" CACHE PATH "" FORCE)
    endif()

    set(AVRO_BUILD_EXECUTABLES OFF CACHE BOOL "" FORCE)
    set(AVRO_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(AVRO_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(AVRO_BUILD_STATIC ON CACHE BOOL "" FORCE)

    FetchContent_Declare(
        avrocpp
        GIT_REPOSITORY https://github.com/apache/avro.git
        GIT_TAG        release-1.12.1
        GIT_SHALLOW    TRUE
        SOURCE_SUBDIR  lang/c++
    )

    set(_ibex_saved_suppress_dev_warnings "${CMAKE_SUPPRESS_DEVELOPER_WARNINGS}")
    set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS 1)
    FetchContent_MakeAvailable(avrocpp)
    set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS "${_ibex_saved_suppress_dev_warnings}")

    if(TARGET avrocpp_s)
        set(IBEX_KAFKA_AVRO_TARGET avrocpp_s)
        ibex_silence_external_target(avrocpp_s)
    elseif(TARGET avrocpp)
        set(IBEX_KAFKA_AVRO_TARGET avrocpp)
        ibex_silence_external_target(avrocpp)
    else()
        message(FATAL_ERROR "Vendored Avro C++ build did not expose avrocpp_s or avrocpp target.")
    endif()
endif()

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

target_compile_definitions(ibex_kafka_plugin
    PRIVATE
        IBEX_KAFKA_ENABLE_CURL=1
)

target_link_libraries(ibex_kafka_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
        nlohmann_json::nlohmann_json
        CURL::libcurl
        $<$<BOOL:${IBEX_KAFKA_AVRO_TARGET}>:${IBEX_KAFKA_AVRO_TARGET}>
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
