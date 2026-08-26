# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

add_library(ibex_ir STATIC
    builder.cpp
    cardinality.cpp
    canonicalize.cpp
    expr_predicates.cpp
    extern_sources.cpp
    join_pushdown.cpp
    join_semi_reduction.cpp
    join_order.cpp
    join_output.cpp
    join_reorder.cpp
    mapped_join_keys.cpp
    node.cpp
    nullability.cpp
    optimizer.cpp
    pending_order.cpp
    predicate_pushdown_pass.cpp
    required_columns.cpp
    column_origins.cpp
    distinct_key_reduction.cpp
    group_key_reduction.cpp
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
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_ir PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_ir
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
