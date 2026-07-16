add_library(ibex_ir STATIC
    builder.cpp
    cardinality.cpp
    canonicalize.cpp
    expr_predicates.cpp
    extern_sources.cpp
    join_pushdown.cpp
    node.cpp
    optimizer.cpp
    required_columns.cpp
    scan_predicates.cpp
    schema.cpp
)
add_library(Ibex::ir ALIAS ibex_ir)

target_include_directories(ibex_ir
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(ibex_ir
    PUBLIC
        Ibex::core
    PRIVATE
        Ibex::CompilerOptions
        fmt::fmt
        spdlog::spdlog
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_ir PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_ir
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
