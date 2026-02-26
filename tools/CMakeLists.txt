add_executable(ibex_repl_bin ibex_repl.cpp)

set_target_properties(ibex_repl_bin PROPERTIES
    OUTPUT_NAME ibex
)

target_link_libraries(ibex_repl_bin
    PRIVATE
        Ibex::repl
        Ibex::CompilerOptions
        CLI11::CLI11
        fmt::fmt
        spdlog::spdlog
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_repl_bin PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_repl_bin
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
)

add_executable(ibex_bench ibex_bench.cpp)

target_include_directories(ibex_bench PRIVATE "${PROJECT_SOURCE_DIR}/libs/csv")

target_link_libraries(ibex_bench
    PRIVATE
        Ibex::parser
        Ibex::runtime
        Ibex::CompilerOptions
        CLI11::CLI11
        fmt::fmt
        rapidcsv
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_bench PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_bench
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
)

add_library(ibex_gen_tf_data_plugin SHARED gen_tf_data.cpp)

set_target_properties(ibex_gen_tf_data_plugin PROPERTIES
    OUTPUT_NAME gen_tf_data
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_gen_tf_data_plugin
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}"
)

target_link_libraries(ibex_gen_tf_data_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_gen_tf_data_plugin PRIVATE Ibex::Sanitizers)
endif()

add_executable(ibex_compile_bin ibex_compile.cpp)

set_target_properties(ibex_compile_bin PROPERTIES
    OUTPUT_NAME ibex_compile
)

target_link_libraries(ibex_compile_bin
    PRIVATE
        Ibex::codegen
        Ibex::parser
        Ibex::CompilerOptions
        CLI11::CLI11
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_compile_bin PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_compile_bin
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
)
