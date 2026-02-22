add_library(ibex_repl STATIC
    repl.cpp
)
add_library(Ibex::repl ALIAS ibex_repl)

target_include_directories(ibex_repl
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(ibex_repl
    PUBLIC
        Ibex::parser
        Ibex::runtime
    PRIVATE
        Ibex::CompilerOptions
        fmt::fmt
        spdlog::spdlog
        dl
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_repl PRIVATE Ibex::Sanitizers)
endif()
