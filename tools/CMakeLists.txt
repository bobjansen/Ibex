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

target_link_libraries(ibex_bench
    PRIVATE
        Ibex::parser
        Ibex::runtime
        Ibex::CompilerOptions
        CLI11::CLI11
        fmt::fmt
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_bench PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_bench
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
)

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
