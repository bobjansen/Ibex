add_library(ibex_core STATIC
    column.cpp
)
add_library(Ibex::core ALIAS ibex_core)

target_include_directories(ibex_core
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(ibex_core
    PUBLIC
        # robin_hood is header-only and appears in public headers (e.g.
        # core/column.hpp's categorical index_map), so its include path must
        # propagate to every consumer. Linking it PUBLIC here reaches ir,
        # parser, runtime, codegen, repl, tools, tests, and plugins transitively.
        robin_hood::robin_hood
    PRIVATE
        Ibex::CompilerOptions
        fmt::fmt
        spdlog::spdlog
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_core PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_core
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
