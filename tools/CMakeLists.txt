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
